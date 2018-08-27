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
    // send a heart beat message to Pandora to monitor Spark Streaming's health.
    val monitor = new StreamingMonitor(appName)

    val context = new StreamingContext(conf, Seconds(option.checkpointInterval))
    context.addStreamingListener(monitor)
    context.checkpoint(option.checkpointDirectory)

    val stream = KinesisInputDStream.builder
      .streamingContext(context)
      .regionName(option.region)
      .streamName(option.stream)
      .endpointUrl(option.endPointURL)
      .checkpointAppName(s"ripple-${option.appName}-kinesis")
      .checkpointInterval(Seconds(option.checkpointInterval))
      .initialPosition(InitialPositionInStream.LATEST)
      // get first 38 bytes as id
      .buildWithMessageHandler {
      (rec) =>
        val r = Record.parseFrom(rec.getData().array())
        val id = rec.getSequenceNumber.substring(38).toLong
        (r, id)
    }
      .filter {
        case (r, id) =>
          // keep `11418793` both available on production and preproduction
          if (option.appName == "preproduction") {
            namespaces.contains(r.namespace) && (r.appId == 11418793 || r.appId < 500)
          } else {
            namespaces.contains(r.namespace)
          }
        case _ => false
      }

    val stateSpec =
      StateSpec
        .function {
          (key: (Int, String, String), value: Option[(Record, Long)], state: State[SessionState]) =>
            updateSessions(key, value, state, option.appName)
        }
        // if after `time` out, no events of existing keys arrive, the `updateSessions` function
        // will be invoked for the last time and then remove these key.
        .timeout(Seconds(60))

    stream.map {
      case (rec, id) => // set aid, cid and sid as partition key
        ((rec.appId, rec.clientId, rec.sessionId), (rec, id))
    }
      .mapWithState(stateSpec)
      .flatMap(s => s)
      .foreachRDD { (rdd) =>
        rdd.foreachPartition { (states) =>
          // write SessionState into DynamoDB's fact and user table
          DataWriter.writeSessionState(appName, states)
        }
      }

    logger.info(s"event=start_running_streaming_task")
    context.start()
    context.awaitTermination()
  }

  /**
    * Update key's SessionState and return the key's SessionState's collection
    *
    * @param key
    * @param value
    * @param state
    * @param appName
    * @return
    */
  def updateSessions(
                      key: (Int, String, String),
                      value: Option[(Record, Long)],
                      state: State[SessionState],
                      appName: String
                    ): Seq[SessionState] = {
    // If event of the key arrive within the timeout's duration, the key's value is defined
    if (value.isDefined) {
      val (rec, id) = value.get

      val nst =
        if (!state.exists) { // state not exists, new session, add `enterApp` ui event.
          Seq(SessionState(events = Seq(SessionState.makeOpenRecord(rec), rec)))
        } else {
          // state exists, session exists, if the gap between two events are beyond 60s
          // then split them into two sessions, otherwise add this event to old session
          val st = state.get
          if (st.events.last.timestamp + 60000 < rec.timestamp) {
            Seq(st.copy(closed = true), SessionState(events = Seq(SessionState.makeOpenRecord(rec), rec)))
          } else {
            // make sure timestamp of events are monotonically increasing
            val ts = math.max(rec.timestamp, st.events.last.timestamp + 1)
            Seq(st.copy(events = (st.events :+ rec.copy(ts))))
          }
        }

      val res = nst.map {
        (st) =>
          if (st.openId.isEmpty) {
            // set sessionId and openId
            st.copy(sessionId = id, openId = st.events.last.values.getOrElse("uoid", ""))
          } else {
            st
          }
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
        case s if s.shouldEmit => s
      }
    } else if (state.exists && state.get.shouldEmit) {
      // no event of the key arrive in timeout's duration, so there doesn't exist its value
      val st = state.get
      Seq(DataWriter.countRecordsAndNotify(appName, st.copy(closed = true)))
    } else Seq()
  }
}