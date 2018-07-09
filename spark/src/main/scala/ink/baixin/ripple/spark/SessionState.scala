package ink.baixin.ripple.spark

import ink.baixin.ripple.core.models.{Record, Session, User}

case class SessionState(
                         timestamp: Long = 0,
                         sessionId: Long = 0,
                         openId: String = "",
                         events: Seq[Record] = Seq()
                       ) {
  def appId = events.head.appId

  private def getUserProfile =
    events.find(_.values.contains("unn")).map { rec =>
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

  private def getUserGeoLocation =
    events.find { rec =>
      rec.values.contains("uct") || rec.values.contains("upn") || rec.values.contains("cuy")
    } map { rec =>
      User.GeoLocation(
        rec.values.getOrElse("uct", ""),
        rec.values.getOrElse("upn", ""),
        rec.values.getOrElse("ucy", "")
      )
    }

  private def getSessionEntrance = {
    val scene = events.find(_.values.contains("scv"))
    val referrer = events.find(_.values.contains("ref"))

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

  private def getSessionGPSLocation =
    events.map { event =>
      val lgt = event.values.getOrElse("lgt", "")
      val lat = event.values.getOrElse("lat", "")
      val acc = event.values.getOrElse("acc", "")
      if (lgt.isEmpty || lat.isEmpty || acc.isEmpty) {
        None
      } else {
        Some(lgt.toDouble, lat.toDouble, acc.toDouble)
      }
    } reduce { (a, b) =>
      if (a.isEmpty) b
      else if (b.isEmpty) a
      else if (a.get._3 < b.get._3) a
      else b
    } collect {
      case (a, b, c) if c > 0.0 => Session.GPSLocation(a, b, c)
    }

  private def getSessionAggregation = Some(
    Session.Aggregation(
      ((events.last.timestamp - events.head.timestamp) / 1000).toInt,
      events.count(_.values.getOrElse("et", "") == "pv"),
      events.count { e =>
        e.values.getOrElse("et", "") == "ui" && e.values.getOrElse("est", "").toLowerCase.startsWith("share")
      },
      events.count { e =>
        e.values.getOrElse("et", "") == "ui" && e.values.getOrElse("est", "").toLowerCase.startsWith("like")
      },
      events.count(e => Seq("ui", "pv").contains(e.values.getOrElse("et", "")))
    )
  )

  private def getSessionEvents = {
    val evs = events.map { event =>
      (
        event.timestamp,
        event.values.getOrElse("et", ""),
        event.values.getOrElse("est", ""),
        event.values.getOrElse("epn", ""),
        event.values.getOrElse("eep", "")
      )
    }

    val uis = evs.collect {
      case (ts, "ui", est, epn, eep) =>
        Session.Event(ts, 0, "ui", est, epn, eep)
    }

    val pvs = evs.foldLeft(Seq[Session.Event]()) {
      (res, tup) =>
        if (res.isEmpty) {
          tup match {
            case (ts, et, est, epn, eep) if et == "pv" || et == "hb" =>
              res :+ Session.Event(ts, 0, "pv", est, epn, eep)
            case _ => res
          }
        } else {
          val dwellTime = (tup._1 - res.last.timestamp) / 1000
          val nres =
            res.updated(res.length - 1, res.last.copy(dwellTime = dwellTime.toInt))
          tup match {
            case (ts, "pv", est, epn, eep) =>
              // merge adjacent events if they are occuring in a short period
              // and having exactly the same parameter
              if (ts - res.last.timestamp < 1000
              && res.last.subType == est
              && res.last.parameter == epn
              && res.last.extraParameter == eep) nres
              else nres :+ Session.Event(ts, 0, "pv", est, epn, eep)
            case _ => nres
          }
        }
    }
    (pvs ++ uis).sortBy(_.timestamp)
  }

  def getUser = User(
    appId,
    openId,
    getUserProfile,
    getUserGeoLocation
  )

  def getSession = Session(
    appId,
    timestamp,
    sessionId,
    openId,
    getSessionEntrance,
    getSessionAggregation,
    getSessionGPSLocation,
    getSessionEvents
  )
}