package ink.baixin.ripple.core

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec
import models._

class FactTableSpec extends FlatSpec {
  private val logger = Logger(this.getClass)
  private val testConfig = ConfigFactory.load.getConfig("ripple-test")

  private lazy val table = {
    val provider = new StateProvider("test-project", testConfig)
    provider.ensureState
    provider.mutator.addSegments(1) // add a new segment

    val segment = provider.listener.getState.get.segments.last
    provider.mutator.provisionSegments(Set(segment.id), true)
    provider.resolver.getFactTable(segment.startTime).get
  }

  private val events = (for (i <- 0 until 5100) yield {
    1526643118000L + 4242 * i
  }).iterator.sliding(10, 5).map { ts =>
    ts.zipWithIndex.map {
      case (t, 0) =>
        Event(t, 0, "pv", "page-a-views", "0")
      case (t, 3) =>
        Event(t, 0, "ui", "share-action", "3")
      case (t, 6) =>
        Event(t, 0, "pv", "page-b-views", "6")
      case (t, 9) =>
        Event(t, 0, "ui", "like-action", "9")
      case (t, _) =>
        Event(t, 0, "hb")
    }
  } toSeq

  private val gpsLocations = for (i <- 0 until 100) yield {
    Session.GPSLocation(i / 7.0, i / 9/0, i / 21.0)
  }

  it should "put sessions successfully" in {
    for ((evs, sid) <- events.zipWithIndex) {
      val res = table.putSession(
        sid % 10,
        evs.head.timestamp,
        sid,
        s"openid-${sid % 13}",
        gpsLocations(sid % 100),
        evs
      )
      assert(res.isSuccess)
    }
  }

  it should "get sessions successfully" in {
    for ((evs, sid) <- events.zipWithIndex) {
      val res = table.getSession(sid % 10, evs.head.timestamp, sid)
      val session = res.get
      assert(session.appId == sid % 10)
      assert(session.timestamp == evs.head.timestamp)
      assert(session.sessionId == sid)
      assert(session.openId == s"openid-${sid % 13}")

      val gps = gpsLocations(sid % 100)
      if (gps.toByteArray.isEmpty) {
        assert(session.gpsLocation.isEmpty)
      } else {
        assert(session.getGpsLocation == gps)
      }

      val cevs = session.getEvents.events
      assert(cevs.size == 4) // only pv and ui are left
      logger.info(s"get_vent=$cevs")
      assert(cevs(0).dwellTime == 4242 * 6 / 1000)
      assert(cevs(2).dwellTime == 5252 * 3 / 1000)

      val aggs = session.getAggregation
      assert(aggs.duration == 4242 * 9 / 1000)
      assert(aggs.pageviews == 2)
      assert(aggs.sharings == 1)
      assert(aggs.likes == 1)
    }
  }

  it should "query sessions successfully" in {
    for (appId <- 0 until 10) {
      events.iterator.sliding(250, 250).foreach { sub =>
        val s = sub.head.head.timestamp
        val e = sub.last.head.timestamp
        val res = table.querySessions(appId, s, e, Seq()).toSeq

        assert(res.size >= sub.size / 10)
        res.foreach { session =>
          assert(session.appId == appId)
          assert(s <= session.timestamp && session.timestamp <= e)
          assert(session.openId.isEmpty)
          assert(session.aggregation.isEmpty)
          assert(session.gpsLocation.isEmpty)
          assert(session.events.isEmpty)
        }
      }
    }
  }

  it should "query sessions with openids successfully" in {
    for (appId <- 0 until 10) {
      (for (i <- 0 to 13) yield s"openid-$i").iterator.sliding(4, 2).foreach {
        openids =>
        val idset = openids.toSet
          val res = table.querySessionsWithOpenIds(appId, 0, Long.MaxValue, idset, Seq("oid")).toSeq
          assert(res.size > 0)
          res.foreach { session =>
            assert(session.appId == appId)
            assert(idset.contains(session.openId))
            assert(session.aggregation.isEmpty)
            assert(session.gpsLocation.isEmpty)
            assert(session.events.isEmpty)
          }
      }
    }
  }
}