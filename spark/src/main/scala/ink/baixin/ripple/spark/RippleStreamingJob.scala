package ink.baixin.ripple.spark

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import ink.baixin.ripple.core.models.Record

object RippleStreamingJob {
  private val logger = Logger(this.getClass)

  def run(option: RippleJobOption) {
    val appName = option.appName
    val namespaces = option.namespaces
    val conf = new SparkConf().setAppName(option.appName)
    val context = new StreamingContext(conf, Seconds(option.checkpointInterval))
    context.checkpoint(option.checkpointDirectory)

    val stream = KinesisInputDStream.builder
      .streamingContext(context)
      .regionName(option.region)
      .streamName(option.stream)
      .endpointUrl(option.endPointURL)
      .checkpointAppName(s"ripple-${option.appName}-kinesis")
      .checkpointInterval(Seconds(option.checkpointInterval))
      .initialPositionInStream(InitialPositionInStream.LATEST)
      .buildWithMessageHandler {
        (rec) =>
          val r = Record.parseFrom(rec.getData().array())
          val id = rec.getSequenceNumber.substring(38).toLong
          (r, id)
      }
      .filter {
        case (r, id) if namespaces.contains(r.namespace) => true
        case _ => false
      }

    val stateSpec =
      StateSpec
        .function {
          (key: (Int, String, String), value: Option[(Record, Long)], state: State[SessionState]) =>
            updateSessions(key, value, state, option.appName)
        }
        .timeout(Seconds(60))

    val sessions =
      stream.map {
        case (rec, id) =>
          ((rec.appId, rec.clientId, rec.sessionId), (rec, id))
      }
        .mapWithState(stateSpec)
        .flatMap(s => s)
        .foreachRDD { (rdd) =>
          rdd.foreachPartition { (states) =>
            DataWriter.writeSessionState(appName, states)
          }
        }

    logger.info(s"event=start_running_streaming_task")
    context.start()
    context.awaitTermination()
  }

  def updateSessions(
                      key: (Int, String, String),
                      value: Option[(Record, Long)],
                      state: State[SessionState],
                      appName: String
                    ): Seq[SessionState] = {
    if (value.isDefined) {
      val (rec, id) = value.get

      val nst =
        if (!state.exists) {
          Seq(SessionState(events = Seq(rec)))
        } else {
          val st = state.get
          if (st.events.last.timestamp + 60000 < rec.timestamp) {
            Seq(st, SessionState(events = Seq(rec)))
          } else {
            Seq(st.copy(events = (st.events :+ rec)))
          }
        }

      val res = nst.map {
        (st) =>
          if (st.openId.isEmpty) {
            val oid = st.events.last.values.getOrElse("uoid", "")
            st.copy(id, openId = oid, events = st.events.sortBy(_.timestamp))
          } else st.copy(events = st.events.sortBy(_.timestamp))
      } map {
        (st) =>
          if (st.shouldEmit) {
            DataWriter.countRecordsAndNotify(appName, st)
          } else {
            st
          }
      }

      state.update(res.last)

      res.collect {
        case s if s.shouldEmit =>
          if (s == res.last) s
          else s.copy(closed = true)
      }
    } else if (state.exists && state.get.shouldEmit) {
      Seq(DataWriter.countRecordsAndNotify(appName, state.get.copy(closed = true)))
    } else Seq()
  }
}