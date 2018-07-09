package ink.baixin.ripple.spark

import java.util.{Timer, TimerTask}

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.models.Record
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object DimeJob {
  private val logger = Logger(this.getClass)

  private def initStateTasks(appName: String) = {
    logger.info("event=init_state_tasks")
    val config = ConfigFactory.load.getConfig(appName)
    val sp = new StateProvider(appName, config)
    sp.ensureState
    val timer = new Timer()
    val task = new TimerTask {
      override def run(): Unit = {
        logger.info("event=ensure_state_segments")
        sp.mutator.ensureSegments
        // get resource in ahead of time to materialize them
        sp.resolver.getUserTable
        sp.resolver.getSegmentTables
      }
    }
    logger.info("event=ensure_state_segments")
    sp.mutator.ensureSegments
    // get resource in ahead of time to materialize them
    sp.resolver.getUserTable
    sp.resolver.getSegmentTables
    // ensure segments every hour
    timer.schedule(task, 3600 * 1000, 3600 * 1000)
    task
  }

  def main(args: Array[String]): Unit = {
    val option = DimeJobOptionParser.parse(args, DimeJobOption()).get
    val task = initStateTasks(option.appName)
    val conf = new SparkConf().setAppName(option.appName)
    val context = new StreamingContext(conf, Seconds(option.checkpointInterval))
    context.checkpoint(option.checkpointDirectory)

    val stream = KinesisInputDStream.builder
      .streamingContext(context)
      .regionName(option.region)
      .streamName(option.stream)
      .endpointUrl(option.endPointURL)
      .checkpointAppName(option.appName)
      .checkpointInterval(Seconds(option.checkpointInterval))
      .initialPositionInStream(InitialPositionInStream.LATEST)
      .buildWithMessageHandler { rec =>
        val r = Record.parseFrom(rec.getData.array)
        val id = rec.getSequenceNumber.substring(38).toLong
        (r, id)
      }

    val stateSpec = StateSpec
      .function(updateSessions _)
      .timeout(Seconds(120))

    stream
      .map {
        case (rec, id) =>
          ((rec.appId, rec.clientId, rec.sessionId), (rec, id))
      }.mapWithState(stateSpec)
      .flatMap(s => s)
      .foreachRDD { rdd =>
        rdd.foreachPartition { states =>
          SessionStateWriter.getWriter(option.appName).put(states)
        }
      }

    logger.info("event=start_running_streaming_task")
    context.start
    context.awaitTermination
    task.cancel
  }

  private def updateSessions(
                            key: (Int, String, String),
                            value: Option[(Record, Long)],
                            state: State[SessionState]
                            ): Seq[SessionState] = {
    if (value.isDefined) {
      val (rec, id) = value.get
      val nst =
        if (!state.exists) {
          Seq(SessionState(events = Seq(rec)))
        } else {
          val st = state.get
          if (st.events.last.timestamp + 60 * 1000 < rec.timestamp) {
            Seq(st, SessionState(events = Seq(rec)))
          } else {
            Seq(st.copy(events = st.events :+ rec))
          }
        }

      val res = nst.map { st =>
        if (st.openId.isEmpty) {
          val oid = st.events.last.values.getOrElse("uoid", "")
          st.copy(st.events.head.timestamp, id, openId = oid, events = st.events.sortBy(_.timestamp))
        } else st.copy(events = st.events.sortBy(_.timestamp))
      }

      state.update(res.last)

      res.collect {
        case s if !s.openId.isEmpty => s
      }
    } else if (state.exists && !state.get.openId.isEmpty) {
      Seq(state.get)
    } else Seq()
  }
}