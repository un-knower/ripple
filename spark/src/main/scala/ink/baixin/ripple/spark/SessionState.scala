package ink.baixin.ripple.spark

import ink.baixin.ripple.core.models.{ Record, Session, User }

case class SessionState(
                         timestamp: Long = 0,
                         sessionId: Long = 0,
                         openId: String = "",
                         events: Seq[Record] = Seq(),
                         notified: Map[String, Long] = Map(),
                         closed: Boolean = false
                       ) {
  def appId = events.head.appId
  def namespace = events.head.namespace
  def getTimestamp =
    if (timestamp > 0) timestamp
    else {
      events.collect {
        case e if (e.values.getOrElse("et", "") == "pv") || (e.values.getOrElse("et", "") == "ui") =>
          e.timestamp
      } match {
        case s if !s.isEmpty => s.min
        case _ => 0L
      }
    }

  def shouldEmit = (
    (!openId.isEmpty) && (timestamp > 0) &&
      (events.find((e) => e.values.getOrElse("et", "") == "pv").isDefined ||
        events.find((e) => e.values.getOrElse("et", "") == "ui").isDefined))

  private def getUserProfile = {
    events.find((rec) => rec.values.contains("unn")).map {
      (rec) =>
        User.Profile(
          rec.values.getOrElse("unn", ""),
          rec.values.getOrElse("uau", ""),
          rec.values.getOrElse("ula", ""),
          rec.values.get("ugd") match {
            case Some("1") => 1
            case Some("2") => 2
            case _ => 0
          }
        )
    }
  }

  private def getUserGeoLocation = {
    events.find {
      (rec) => (rec.values.contains("uct") || rec.values.contains("upn") || rec.values.contains("ucy"))
    } map {
      (rec) =>
        User.GeoLocation(
          rec.values.getOrElse("uct", ""),
          rec.values.getOrElse("upn", ""),
          rec.values.getOrElse("ucy", "")
        )
    }
  }

  private def getSessionEntrance = {
    val scene = events.find((e) => e.values.contains("scv"))
    val referrer = events.find((e) => e.values.contains("ref"))

    Some(Session.Entrance(
      scene match {
        case Some(rec) => rec.values("scv").toInt
        case None => 0
      },
      referrer match {
        case Some(rec) => rec.values("ref")
        case None => ""
      }
    ))
  }

  private def getSessionGPSLocation = {
    events.map {
      (event) =>
        val lgt = event.values.getOrElse("lgt", "")
        val lat = event.values.getOrElse("lat", "")
        val acc = event.values.getOrElse("acc", "")
        if (lgt.isEmpty || lat.isEmpty || acc.isEmpty) {
          None
        } else {
          Some((lgt.toDouble, lat.toDouble, acc.toDouble))
        }
    } reduce {
      (a, b) =>
        if (a.isEmpty) b
        else if (b.isEmpty) a
        else if (a.get._3 < b.get._3) a
        else b
    } collect {
      case (a, b, c) if c > 0.0 =>
        Session.GPSLocation(a, b, c)
    }
  }

  private def getSessionAggregation(eve: Seq[Session.Event]) = Some(
    Session.Aggregation(
      ((events.last.timestamp - eve.head.timestamp) / 1000).toInt,
      eve.count(_.`type` == "pv"),
      eve.count(e =>
        e.`type` == "ui" && e.subType.toLowerCase.startsWith("share")
      ),
      eve.count(e =>
        e.`type` == "ui" && e.subType.toLowerCase.startsWith("like")
      ),
      eve.size
    )
  )

  def getSessionEvents = {
    val evs = events.map {
      (event) =>
        (
          event.timestamp,
          event.values.getOrElse("et", ""),
          event.values.getOrElse("est", ""),
          event.values.getOrElse("epn", ""),
          event.values.getOrElse("eep", ""),
          event.repeatTimes
        )
    }

    val uis = evs.collect {
      case (ts, "ui", est, epn, eep, cnt) if cnt > 0 =>
        Session.Event(ts, 0, "ui", est, epn, eep, cnt)
    }

    val pvs = evs.foldLeft(Seq[Session.Event]()) {
      (res, tup) =>
        if (res.isEmpty) {
          tup match {
            case (ts, "pv", est, epn, eep, cnt) if cnt > 0 =>
              res :+ Session.Event(ts, 0, "pv", est, epn, eep, cnt)
            case _ => res
          }
        } else {
          val dwellTime = {
            val millis = (tup._1 - res.last.timestamp)
            millis / 1000 + (if (millis % 1000 >= 500) 1 else 0)
          }
          val nres =
            res.updated(res.length - 1, res.last.copy(dwellTime = dwellTime.toInt))
          tup match {
            case (ts, "pv", est, epn, eep, cnt) if cnt > 0 =>
              // merge adjacent events if they are occuring in a short period
              // and having exactly the same parameter
              nres :+ Session.Event(ts, 0, "pv", est, epn, eep, cnt)
            case _ => nres
          }
        }
    }

    (pvs ++ uis).sortBy(_.timestamp)
  }

  def getUser =
    User(
      appId,
      openId,
      getUserProfile,
      getUserGeoLocation
    )

  def getSession = {
    val eve = getSessionEvents
    Session(
      appId,
      getTimestamp,
      sessionId,
      openId,
      getSessionEntrance,
      getSessionAggregation(eve),
      getSessionGPSLocation,
      eve
    )
  }
}