package ink.baixin.ripple.core.documents

import java.nio.ByteBuffer

import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.spec.{GetItemSpec, QuerySpec}
import ink.baixin.ripple.core.models._

import scala.util.{Failure, Success, Try}


class FactTable(private val table: Table) {

  // TODO: why use sessionId?
  private def getSortKey(timestamp: Long, sessionId: Long) = {
    val bf = ByteBuffer.allocate(16)
    bf.putLong(timestamp)
    bf.putLong(sessionId)
    val sk = Array[Byte](8)
    bf.get(sk)
    sk
  }

  private def getSessionKey(appId: Int, sid: Array[Byte]) = {
    val bf = ByteBuffer.wrap(sid)
    val timestamp = bf.getLong
    val sessionId = bf.getLong
    Session.Key(appId, timestamp, sessionId)
  }

  private def safeQuery(spec: QuerySpec) =
    Try(table.query(spec).iterator) match {
      case Success(iter) =>
        new Iterator[Item] {
          override def hasNext() = iter.hasNext

          override def next() = iter.next
        }
      case Failure(e) =>
        Seq[Item]().iterator
    }

  def putSession(primaryKey: Int,
                 sortKey: Array[Byte],
                 openId: String,
                 agg: Session.Aggregation,
                 events: Session.Events) = Try {
    val item = new Item()
      .withPrimaryKey("aid", primaryKey, "sid", sortKey)
      .withString("oid", openId)
      .withBinary("agg", agg.toByteArray)
      .withBinary("evs", events.toByteArray)
    table.putItem(item)
  }

  def putSession(primaryKey: Int,
                 sortKey: Array[Byte],
                 openId: String,
                 agg: Session.Aggregation,
                 gps: Session.GPSLocation,
                 events: Session.Events) = Try {
    val item = new Item()
      .withPrimaryKey("aid", primaryKey, "sid", sortKey)
      .withString("oid", openId)
      .withBinary("agg", agg.toByteArray)
      .withBinary("gps", gps.toByteArray)
      .withBinary("evs", events.toByteArray)
    table.putItem(item)
  }

  def putSession(session: Session) {
    val key = session.getKey
    val pk = key.appId
    val sk = getSortKey(key.timestamp, key.sessionId)
    if (session.gpsLocation.isEmpty) {
      putSession(pk, sk, session.getUserKey.openId, session.getAggregation, session.getEvents)
    } else {
      putSession(pk, sk, session.getUserKey.openId, session.getAggregation, session.getGpsLocation, session.getEvents)
    }

  }

  /*  def putSession_(appId: Int,
                    ts: Long,
                    sessionId: Long,
                    uk: User.Key,
                    events: Seq[Event]) = {
      val sorted = events.sortBy(_.timestamp)
      val compacted = sorted.foldLeft(Seq[Event]()) { (evs, ev) =>
        // insert event to tail in the first time
        if (evs.isEmpty) evs :+ ev
        else {
          val nev = evs.updated(
            evs.size - 1,
            // former's dwell time = latter - former
            evs.last.copy(dwellTime = (ev.timestamp - evs.last.timestamp).toInt / 1000)
          )
          // TODO I think this logic should be first checked.
          if (ev.`type` != "hb") nev :+ ev
          else nev
        }
      }

      val aggregation = Session.Aggregation(
        duration = (sorted.last.timestamp - sorted.head.timestamp).toInt / 1000,
        pageviews = compacted.count(_.`type` == "pv"),
        sharings = compacted.count(e => e.`type` == "ui" && e.subType.toLowerCase.startsWith("share")),
        likes = compacted.count(e => e.`type` == "ui" && e.subType.toLowerCase.startsWith("like"))
      )
      val compactedEvents = Session.Events(compacted)
      val sortKey = Array[Byte](8)
      val sortKeyBuffer = ByteBuffer.allocate(8)
      sortKeyBuffer.putLong(ts)
      sortKeyBuffer.putLong(sessionId)
      sortKeyBuffer.get(sortKey)

      putSession(appId, sortKey, uk, aggregation, compactedEvents)
    }*/

  def getSession(key: Session.Key) = Try {
    val pk = key.appId
    val sk = getSortKey(key.timestamp, key.sessionId)
    val spec = new GetItemSpec()
      .withPrimaryKey("aid", pk, "sid", sk)
      .withAttributesToGet("oid", "agg", "evs", "gps")

    val res = table.getItem(spec)
    if (res.hasAttribute("gps")) {
      Session()
        .withKey(key)
        .withUserKey(User.Key(pk, res.getString("oid")))
        .withAggregation(Session.Aggregation.parseFrom(res.getBinary("agg")))
        .withGpsLocation(Session.GPSLocation.parseFrom(res.getBinary("gps")))
        .withEvents(Session.Events.parseFrom(res.getBinary("evs")))
    } else {
      Session()
        .withKey(key)
        .withUserKey(User.Key(pk, res.getString("oid")))
        .withAggregation(Session.Aggregation.parseFrom(res.getBinary("agg")))
        .withEvents(Session.Events.parseFrom(res.getBinary("evs")))
    }
  }

  def queryEvents(appId: Int, start_ts: Long, end_ts: Long) = {
    val pk = appId
    // TODO why set 0 and -1
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    val spec = new QuerySpec().withHashKey("aid", pk)
      .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
      .withAttributesToGet("evs")
    safeQuery(spec).flatMap { item =>
      Session.Events.parseFrom(item.getBinary("evs")).events.iterator
    }
  }

  def querySessions(appId: Int, start_ts: Long, end_ts: Long) = {
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    val spec = new QuerySpec().withHashKey("aid", pk)
      .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
      .withAttributesToGet("sid", "oid", "agg", "gps", "evs")
    safeQuery(spec).map { item =>
      val s = Session()
        .withKey(getSessionKey(appId, item.getBinary("sid")))
        .withUserKey(User.Key(appId, item.getString("oid")))
        .withAggregation(Session.Aggregation.parseFrom(item.getBinary("agg")))
        .withEvents(Session.Events.parseFrom(item.getBinary("evs")))

      if (item.hasAttribute("gps")) {
        s.withGpsLocation(Session.GPSLocation.parseFrom(item.getBinary("gps")))
      } else {
        s
      }
    }
  }

  def queryUserKeys(appId: Int, start_ts: Long, end_ts: Long) = {
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    val spec = new QuerySpec().withHashKey("aid", pk)
      .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
      .withAttributesToGet("oid")

    safeQuery(spec).map { item =>
      User.Key(appId, item.getString("oid"))
    }
  }

  def querySessionKeys(appId: Int, start_ts: Long, end_ts: Long) = {
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    val spec = new QuerySpec().withHashKey("aid", pk)
      .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
      .withAttributesToGet("sid")

    safeQuery(spec).map { item =>
      getSessionKey(appId, item.getBinary("sid"))
    }
  }

  def queryAggregations(appId: Int, start_ts: Long, end_ts: Long) = {
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    val spec = new QuerySpec().withHashKey("aid", pk)
      .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
      .withAttributesToGet("agg")

    safeQuery(spec).map { item =>
      Session.Aggregation.parseFrom(item.getBinary("agg"))
    }
  }

  def queryUserKeysEvents(appId: Int,
                          start_ts: Long,
                          end_ts: Long,
                          openIds: Set[String]) = {
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)
    // we can only have at most 100 values in `in` set predicate
    // if userKeys are larger than this number, we have to split them up
    openIds.iterator.sliding(100, 100).flatMap { oids =>
      val spec = new QuerySpec().withHashKey("aid", pk)
        .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
        .withQueryFilters(new QueryFilter("oid").in(oids: _*))
        .withAttributesToGet("oid", "evs")

      safeQuery(spec).flatMap { item =>
        val uk = User.Key(appId, item.getString("oid"))
        val ev = Session.Events.parseFrom(item.getBinary("evs"))
        ev.events.map(e => (uk, e)).iterator
      }
    }
  }

  def queryUserKeysAggregations(appId: Int,
                                start_ts: Long,
                                end_ts: Long,
                                openIds: Set[String]) = {
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)
    openIds.iterator.sliding(100, 100).flatMap { oids =>
      val spec = new QuerySpec().withHashKey("aid", pk)
        .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
        .withQueryFilters(new QueryFilter("oid").in(oids: _*))
        .withAttributesToGet("oid", "agg")

      safeQuery(spec).map { item =>
        val uk = User.Key(appId, item.getString("oid"))
        val agg = Session.Aggregation.parseFrom(item.getBinary("agg"))
        (uk, agg)
      }
    }
  }

  def queryUserKeysSessions(appId: Int,
                            start_ts: Long,
                            end_ts: Long,
                            openIds: Set[String]) = {
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)
    openIds.iterator.sliding(100, 100).flatMap { oids =>
      val spec = new QuerySpec().withHashKey("aid", pk)
        .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
        .withQueryFilters(new QueryFilter("oid").in(oids: _*))
        .withAttributesToGet("sid", "oid", "agg", "evs")

      safeQuery(spec).map { item =>
        val s = new Session().withKey(getSessionKey(appId, item.getBinary("sid")))
          .withUserKey(User.Key(appId, item.getString("oid")))
          .withAggregation(Session.Aggregation.parseFrom(item.getBinary("agg")))
          .withEvents(Session.Events.parseFrom(item.getBinary("evs")))
        if (item.hasAttribute("gps")) {
          (s.getUserKey, s.withGpsLocation(Session.GPSLocation.parseFrom(item.getBinary("pgs"))))
        } else {
          (s.getUserKey, s)
        }
      }
    }
  }
}