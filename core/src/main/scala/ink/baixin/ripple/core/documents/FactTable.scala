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
                 uk: User.Key,
                 agg: Session.Aggregation,
                 events: Session.Events) = Try {
    val item = new Item()
      .withPrimaryKey("aid", primaryKey, "sid", sortKey)
      .withBinary("uk", uk.toByteArray)
      .withBinary("agg", agg.toByteArray)
      .withBinary("events", events.toByteArray)
    table.putItem(item)
  }

  def putSession(session: Session) {
    val key = session.getKey
    val pk = key.appId
    val sk = getSortKey(key.timestamp, key.sessionId)
    putSession(pk, sk, session.getUserKey, session.getAggregation, session.getEvents)
  }

  def putSession_(appId: Int,
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
  }

  def getSession(key: Session.Key) = Try {
    val pk = key.appId
    val sk = getSortKey(key.timestamp, key.sessionId)
    val spec = new GetItemSpec()
      .withPrimaryKey("aid", pk, "sid", sk)
      .withAttributesToGet("uk", "agg", "events")

    val res = table.getItem(spec)
    Session().withKey(key)
      .withUserKey(User.Key.parseFrom(res.getBinary("uk")))
      .withAggregation(Session.Aggregation.parseFrom(res.getBinary("agg")))
      .withEvents(Session.Events.parseFrom(res.getBinary("events")))
  }

  def queryEvents(appId: Int, start_ts: Long, end_ts: Long) = {
    val pk = appId
    // TODO why set 0 and -1
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    val spec = new QuerySpec().withHashKey("aid", pk)
      .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
      .withAttributesToGet("events")
    safeQuery(spec).flatMap { item =>
      Session.Events.parseFrom(item.getBinary("events")).events.iterator
    }
  }

  def querySessions(appId: Int, start_ts: Long, end_ts: Long) = {
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    val spec = new QuerySpec().withHashKey("aid", pk)
      .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
      .withAttributesToGet("sid", "uk", "agg", "events")
    safeQuery(spec).map { item =>
      Session().withKey(getSessionKey(appId, item.getBinary("sid")))
        .withUserKey(User.Key.parseFrom(item.getBinary("uk")))
        .withAggregation(Session.Aggregation.parseFrom(item.getBinary("agg")))
        .withEvents(Session.Events.parseFrom(item.getBinary("events")))
    }
  }

  def queryUserKeys(appId: Int, start_ts: Long, end_ts: Long) = {
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    val spec = new QuerySpec().withHashKey("aid", pk)
      .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
      .withAttributesToGet("uk")

    safeQuery(spec).map { item =>
      User.Key.parseFrom(item.getBinary("uk"))
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
      val uks = oids.map(o => User.Key(appId, o).toByteArray)
      val spec = new QuerySpec().withHashKey("aid", pk)
        .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
        .withQueryFilters(new QueryFilter("uk").in(uks: _*))
        .withAttributesToGet("uk", "events")

      safeQuery(spec).flatMap { item =>
        val userKey = User.Key.parseFrom(item.getBinary("uk"))
        val ev = Session.Events.parseFrom(item.getBinary("events"))
        ev.events.map((userKey, _)).iterator
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
      val uks = oids.map(o => User.Key(appId, o).toByteArray)
      val spec = new QuerySpec().withHashKey("aid", pk)
        .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
        .withQueryFilters(new QueryFilter("uk").in(uks: _*))
        .withAttributesToGet("uk", "events")

      safeQuery(spec).map { item =>
        val uk = User.Key.parseFrom(item.getBinary("uk"))
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
      val uks = oids.map(o => User.Key(appId, o).toByteArray)
      val spec = new QuerySpec().withHashKey("aid", pk)
        .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
        .withQueryFilters(new QueryFilter("uk").in(uks: _*))
        .withAttributesToGet("sid", "uk", "agg", "events")

      safeQuery(spec).map { item =>
        val s = new Session().withKey(getSessionKey(appId, item.getBinary("sid")))
          .withUserKey(User.Key.parseFrom(item.getBinary("uk")))
          .withAggregation(Session.Aggregation.parseFrom(item.getBinary("agg")))
          .withEvents(Session.Events.parseFrom(item.getBinary("events")))
        (s.getUserKey, s)
      }
    }
  }
}