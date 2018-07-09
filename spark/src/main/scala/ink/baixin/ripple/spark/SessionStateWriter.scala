package ink.baixin.ripple.spark

import java.util.concurrent.atomic.AtomicReference

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.documents.{SegmentTable, UserTable}
import ink.baixin.ripple.core.models.{Session, User}
import ink.baixin.ripple.core.state.State

import scala.collection.mutable

class SessionStateWriter(appName: String) {
  private val logger = Logger(this.getClass)
  private val userTable = new AtomicReference[Option[UserTable]](None)
  private val segmentTables = new AtomicReference[Seq[(State.Segment, SegmentTable)]](Seq())
  private val sp = new StateProvider(appName, ConfigFactory.load.getConfig(appName))

  private lazy val task = sp.listener.start(30 * 1000) {
    case Some(ns) =>
      val newSegs = ns.segments.filter(s => s.provisioned)
      val oldSegs = segmentTables.get.map(_._1)
      if (newSegs != oldSegs) {
        logger.info("event=update_tables reason=segments_changed")
        updateTables
      }
    case _ =>
      logger.warn("event=get_empty_state")
  }

  private def updateTables: Unit = {
    userTable.set(sp.resolver.getUserTable)
    segmentTables.set(sp.resolver.getSegmentTables)
  }

  def initialize = {
    sp.listener.syncAndGetState
    logger.info("event=update_tables reason=initialization")
    updateTables

    logger.info("event=start_listening_task")
    task
    this
  }

  def getSegmentTables(ts: Long) = {
    segmentTables.get.find {
      case (seg, table) if (seg.startTime <= ts && ts < seg.endTime) => true
      case _ => false
    }
  }

  def put(ss: Iterator[SessionState]): Unit = {
    logger.info(s"event=put_session_states project=$appName")
    // only put each session and user once, use two maps to deduplicate
    val userMap = new mutable.HashMap[(Int, String), User]()
    val sessionMap = new mutable.HashMap[(Int, Long, Long), Session]()
    ss.foreach { st =>
      val user = st.getUser
      val session = st.getSession
      userMap.put((user.appId, user.openId), user)
      sessionMap.put((session.appId, session.timestamp, session.sessionId), session)
    }

    userTable.get match {
      case Some(ut) =>
        userMap.values.foreach { u =>
          logger.debug(s"event=put_user project=$appName app_id=${u.appId} open_id=${u.openId}")
          ut.putUser(u)
        }
      case _ =>
        logger.error(s"event=user_table_not_found project=$appName")
    }

    sessionMap.values.foreach { s =>
      getSegmentTables(s.timestamp) match {
        case Some((seg, table)) =>
          logger.debug(s"event=put_session project=$appName app_id=${s.appId} ts=${s.timestamp} seg_id=${seg.id}")
          table.putSession(s)
        case None =>
          logger.error(s"event=segment_table_not_found project=$appName app_id=${s.appId} ts=${s.timestamp}")
      }
    }
  }
}


object SessionStateWriter {
  private val cacheMap = scala.collection.mutable.HashMap[String, SessionStateWriter]()
  def getWriter(appName: String) = {
    cacheMap.getOrElseUpdate(appName, new SessionStateWriter(appName).initialize)
  }
}