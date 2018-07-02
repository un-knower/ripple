package ink.baixin.ripple.core.documents

import java.nio.ByteBuffer

import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.spec.{GetItemSpec, QuerySpec}
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.models._
import scala.util.{Failure, Success, Try}


class FactTable(private val table: Table) {
  private val logger = Logger(this.getClass)
  val allAttributes = Seq("oid", "agg", "evs", "gps")

  private def getSortKey(timestamp: Long, sessionId: Long) = {
    val bf = ByteBuffer.allocate(16)
    bf.putLong(timestamp)
    bf.putLong(sessionId)
    bf.array
  }

  private def parseSessionKey(sid: Array[Byte]) = {
    val bf = ByteBuffer.wrap(sid)
    val timestamp = bf.getLong
    val sessionId = bf.getLong
    (timestamp, sessionId)
  }

  private def safeQuery(spec: QuerySpec) =
    Try(table.query(spec).iterator) match {
      case Success(iter) =>
        new Iterator[Item] {
          override def hasNext() = iter.hasNext

          override def next() = iter.next
        }
      case Failure(e) =>
        logger.error(s"event=safe_query error=$e")
        Seq[Item]().iterator
    }

  def putSession(aid: Int,
                 ts: Long,
                 sid: Long,
                 openId: String,
                 gps: Session.GPSLocation,
                 events: Seq[Event]) = Try {
    // check parameters
    assert(!openId.isEmpty)
    assert(!events.isEmpty)
    logger.debug(s"event=put_session pk=$aid sid=$sid openid=$openId")

    val sorted = events.sortBy(_.timestamp)
    val uis = sorted.filter(_.`type` == "ui")
    val pvs = sorted.foldLeft(Seq[Event]()) { (evs, e) =>
      val prev = if (!evs.isEmpty) {
        evs.updated(
          evs.size - 1,
          evs.last.withDwellTime((e.timestamp - evs.last.timestamp).toInt / 1000)
        )
      } else evs

      if (e.`type` == "pv") prev :+ e else prev
    }
    val merged = (uis ++ pvs).sortBy(_.timestamp)
    val agg = Session.Aggregation(
      duration = (sorted.last.timestamp - sorted.head.timestamp).toInt / 1000,
      pageviews = merged.count(_.`type` == "pv"),
      sharings = merged.count(e => e.`type` == "ui" && e.subType.toLowerCase.startsWith("share")),
      likes = merged.count(e => e.`type` == "ui" && e.subType.toLowerCase.startsWith("like"))
    )

    val sk = getSortKey(ts, sid)
    val evs = Session.Events(merged)
    val keyItem = new Item()
      .withPrimaryKey("aid", aid, "sid", sk)
      .withString("oid", openId)

    val item = Seq(
      ("agg", agg.toByteArray),
      ("gps", gps.toByteArray),
      ("evs", evs.toByteArray)
    ).foldLeft(keyItem) {
      case (it, (attr, value)) if !value.isEmpty => it.withBinary(attr, value)
      case (it, _) => it
    }
    table.putItem(item)
  }

  private def buildSession(pk: Int, ts: Long, sid: Long, attrs: Seq[String], item: Item) =
    attrs.foldLeft(
      Session().withAppId(pk).withTimestamp(ts).withSessionId(sid)
    ) {
      case (s, "oid") if item.hasAttribute("oid") =>
        s.withOpenId(item.getString("oid"))
      case (s, "agg") if item.hasAttribute("agg") =>
        s.withAggregation(Session.Aggregation.parseFrom(item.getBinary("agg")))
      case (s, "gps") if item.hasAttribute("pgs") =>
        s.withGpsLocation(Session.GPSLocation.parseFrom(item.getBinary("pgs")))
      case (s, "evs") if item.hasAttribute("evs") =>
        s.withEvents(Session.Events.parseFrom(item.getBinary("evs")))
      case (s, _) => s
    }

  def getSession(appId: Int, ts: Long, sid: Long, attrs: Seq[String] = allAttributes) =
    Try {
      logger.debug(s"event=get_session pk=$appId ts=$ts sid=$sid attrs=$attrs")
      val pk = appId
      val sk = getSortKey(ts, sid)
      val spec = new GetItemSpec()
        .withPrimaryKey("aid", pk, "sid", sk)
        .withAttributesToGet(attrs: _*)
      buildSession(appId, ts, sid, attrs, table.getItem(spec))
    }

  def querySessions(appId: Int, start_ts: Long, end_ts: Long, attrs: Seq[String] = allAttributes) = {
    logger.debug(s"event=query_sessions pk=$appId start=$start_ts end=$end_ts attrs=$attrs")
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    val spec = new QuerySpec()
      .withHashKey("aid", pk)
      .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
      .withAttributesToGet((attrs :+ "sid"): _*)

    safeQuery(spec).map { item =>
      val (ts, sid) = parseSessionKey(item.getBinary("sid"))
      buildSession(pk, ts, sid, attrs, item)
    }
  }

  def querySessionsWithOpenIds(appId: Int,
                               start_ts: Long,
                               end_ts: Long,
                               openIds: Set[String],
                               attrs: Seq[String] = allAttributes) = {
    logger.debug(s"event=query_sessions_with_openids pk=$appId start=$start_ts end=$end_ts openids=$openIds attrs=$attrs")
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    // we can only have at most 100 values in `in` set predicate
    // if userKeys are larger than this number, we have to split them up
    openIds.iterator.sliding(100, 100).flatMap { oids =>
      val spec = new QuerySpec()
        .withHashKey("aid", pk)
        .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
        .withQueryFilters(new QueryFilter("oid").in(oids: _*))
        .withAttributesToGet((attrs :+ "sid"): _*)

      safeQuery(spec).map { item =>
        val (ts, sid) = parseSessionKey(item.getBinary("sid"))
        buildSession(pk, ts, sid, attrs, item)
      }
    }
  }
}