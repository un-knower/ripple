package ink.baixin.ripple.spark

import scala.collection.mutable.HashMap
import scala.util.{Failure, Success, Try}
import com.typesafe.scalalogging.Logger
import com.typesafe.config.ConfigFactory
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.models._

class DataWriter(appName: String) {
  private val logger = Logger(this.getClass)

  private val config = ConfigFactory.load().getConfig("ripple")
  private val sp = new StateProvider(appName, config)

  private val userTable = sp.resolver.getUserTable
  private val factTable = sp.resolver.getFactTable
  private val countTable = sp.resolver.getCountTable
  private val aggTable = sp.resolver.getAggregationTable

  private lazy val notifier = {
    val notifyConfig = config.getConfig(s"notifier-${appName}")
    val uri = new java.net.URI(notifyConfig.getString("uri"))
    val user = notifyConfig.getString("user")
    val pass = notifyConfig.getString("pass")

    new Notifier(uri, user, pass)
  }

  private def getRecordInfo(rec: Record) = Try {
    val et = rec.values("et")
    assert(et == "pv" || et == "ui")

    import spray.json._
    val param = if (rec.values.getOrElse("eep", "").trim.isEmpty) "{}" else rec.values("eep")
    val key = param.parseJson.asJsObject.getFields("key") match {
      case Seq(JsString(key)) => key
      case _ =>
        if (et == "pv") rec.values("est")
        else s"${rec.values("est")}${rec.values.getOrElse("epn", "")}"
    }

    (et, key)
  }

  private def getEventKey(eve: Session.Event) = Try {
    assert(eve.`type` == "pv" || eve.`type` == "ui")

    import spray.json._
    val param = if (eve.extraParameter.trim.isEmpty) "{}" else eve.extraParameter
    param.parseJson.asJsObject.getFields("key") match {
      case Seq(JsString(key)) => key
      case _ =>
        if (eve.`type` == "pv") eve.subType
        else s"${eve.subType}${eve.parameter}"
    }
  }

  def putSessionState(ss: Iterator[SessionState]) {
    logger.info(s"event=put_session_states app_name=$appName")

    // only put each session and user once, use two maps to deduplicate
    val userMap = new scala.collection.mutable.HashMap[(Int, String), User]()
    val sessionMap = new scala.collection.mutable.HashMap[(Int, Long, Long), Session]()

    ss.foreach {
      (st) =>
        try {
          val user = st.getUser
          val session = st.getSession

          if (!session.aggregation.isEmpty && !session.events.isEmpty && session.getAggregation.total > 0) {
            userMap.put((user.appId, user.openId), user)
            sessionMap.put((session.appId, session.timestamp, session.sessionId), session)
          }
        } catch {
          case e: Throwable =>
            logger.error(s"event=get_session_error error=$e")
        }
    }

    userTable match {
      case Some(ut) =>
        userMap.values.foreach {
          (u) =>
            logger.debug(s"event=put_user app_name=$appName app_id=${u.appId} open_id=${u.openId}")
            ut.putUser(u)
        }
      case _ =>
        logger.error(s"event=user_table_not_found app_name=$appName")
    }

    factTable match {
      case Some(table) =>
        sessionMap.values.foreach {
          (s) =>
            logger.debug(s"event=put_session app_name=$appName app_id=${s.appId} ts=${s.timestamp}")
            table.putSession(s)
        }
      case None =>
        logger.error(s"event=cannot_put_session app_name=$appName")
    }
  }

  def putAggregations(
                       appId: Int, openId: String, aggs: Seq[AggregationRecord], topItems: Map[String, Seq[TopItemRecord]]
                     ) {
    aggTable match {
      case Some(table) =>
        logger.debug(s"event=put_aggregations app_name=$appName app_id=${appId}")
        table.putAggregations(appId, openId, aggs, topItems)
      case None =>
        logger.error(s"event=cannot_put_aggregations app_name=$appName")
    }
  }

  /**
    * filter `pv` and `ui` records and store same openid's same records' repeated time on DynamoDB
    *
    * @param appId
    * @param openId
    * @param record
    * @return
    */
  private def countRecord(appId: Int, openId: String, record: Record) = {
    getRecordInfo(record) match {
      case Success((et, key)) =>
        if (countTable.nonEmpty) {
          countTable.get.increaseCount(appId, openId, et, key, record.timestamp) match {
            case Success(res) =>
              logger.debug(s"event=count_record record=$record count=${res.count}")
              record.copy(repeatTimes = res.count)
            case Failure(e) =>
              logger.warn(s"event=cannot_count_record reason=$e record=$record")
              record.copy(repeatTimes = -1)
          }
        } else {
          logger.warn(s"event=cannot_count_record reason=no_count_table record=$record")
          record
        }
      case Failure(e) =>
        record
    }
  }

  def countRecords(state: SessionState) = {
    logger.info(s"event=count_records app_name=$appName")
    val events = state.events.map {
      record =>
        if (record.repeatTimes == 0) countRecord(state.appId, state.openId, record)
        else record
    }
    state.copy(events = events)
  }

  def notifyRecords(state: SessionState) = Try {
    logger.info(s"event=notify_records app_name=$appName")
    // save notified event's timestamp in a Map, and only event after that will be notifying.
    val notifyState = (new HashMap() ++ state.notified)
    val events = state.getSessionEvents
    val lastPV = {
      val pvEvents = events.filter(_.`type` == "pv")
      if (!pvEvents.isEmpty) pvEvents.last
      else null
    }
    events.foreach {
      (eve) =>
        getEventKey(eve).foreach {
          (key) =>
            val et = eve.`type`
            // for `pv` event, if this SessionState is not closed, the last pv event will not be sent
            // because we must close the SessionState then can we calculate last pv's dwell time, so
            // if SessionState is not close, last pv event is not sent
            val shouldSendPV = (et == "pv" && (eve != lastPV || state.closed))
            // each `ui` event will be sent
            if (notifyState.getOrElse(key, 0L) < eve.timestamp && (et == "ui" || shouldSendPV)) {
              notifier.send(state.appId, state.openId, eve)
              // after send event, store its timestamp in the Map
              notifyState.put(key, eve.timestamp)
            }
        }
    }
    state.copy(notified = notifyState.toMap)
  }
}

object DataWriter {
  private val cacheMap = scala.collection.mutable.HashMap[String, DataWriter]()

  private def getWriter(appName: String) = {
    cacheMap.getOrElseUpdate(appName, new DataWriter(appName))
  }

  def writeSessionState(appName: String, iter: Iterator[SessionState]) {
    import scala.collection.mutable._
    getWriter(appName).putSessionState(iter)
  }

  def writeAggregations(
                         appName: String, appId: Int, openId: String,
                         records: Seq[AggregationRecord], topItems: Map[String, Seq[TopItemRecord]]
                       ) {
    getWriter(appName).putAggregations(appId, openId, records, topItems)
  }

  def countRecordsAndNotify(appName: String, state: SessionState) = {
    val writer = getWriter(appName)
    writer.notifyRecords(writer.countRecords(state)).getOrElse(state)
  }
}