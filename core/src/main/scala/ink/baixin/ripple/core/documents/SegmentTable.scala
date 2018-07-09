package ink.baixin.ripple.core.documents

import java.nio.ByteBuffer

import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.spec.{GetItemSpec, QuerySpec}
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.models._
import scala.util.{Failure, Success, Try}


class SegmentTable(private val table: Table) {
  private val logger = Logger(this.getClass)
  val allAttributes = Seq("oid", "ent", "agg", "evs", "gps")

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

  private def packField(field: Any) = {
    field match {
      case Some(f: Session.Entrance) => f.toByteArray
      case Some(f: Session.Aggregation) => f.toByteArray
      case Some(f: Session.GPSLocation) => f.toByteArray
      case Seq(events @ _*) =>
        val evs = events.collect {
          case e: Session.Event => e
        }
        Session.EventsPack(
          evs.map(_.timestamp),
          evs.map(_.dwellTime),
          evs.map(_.`type`),
          evs.map(_.subType),
          evs.map(_.parameter),
          evs.map(_.extraParameter)
        ).toByteArray
      case _ => Array[Byte](0)
    }
  }

  private def packSession(pk: Int, ts: Long, sid: Long, attrs: Seq[String], item: Item) =
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
        val eventsPack = Session.EventsPack.parseFrom(item.getBinary("evs"))
        val events = for (i <- 0 until eventsPack.timestamp.size) yield {
          Session.Event(
            eventsPack.timestamp(i),
            eventsPack.dwellTime(i),
            eventsPack.`type`(i),
            eventsPack.subType(i),
            eventsPack.parameter(i),
            eventsPack.extraParameter(i)
          )
        }
        s.withEvents(events)
      case (s, _) => s
    }

  def putSession(session: Session) = Try {
    // check parameters
    assert(!session.openId.isEmpty)
    assert(!session.events.isEmpty)
    logger.debug(s"event=put_session pk=${session.appId} ts=${session.timestamp} sid=${session.sessionId} openid=${session.openId}")

    val keyItem = new Item()
      .withPrimaryKey("aid", session.appId, "sid", getSortKey(session.timestamp, session.sessionId))
      .withString("oid", session.openId)

    val item = Seq(
      ("ent", packField(session.entrance)),
      ("agg", packField(session.aggregation)),
      ("gps", packField(session.gpsLocation)),
      ("evs", packField(session.events))
    ).foldLeft(keyItem) {
      case (it, (attr, value)) if !value.isEmpty => it.withBinary(attr, value)
      case (it, _) => it
    }
    table.putItem(item)
  }

  def getSession(appId: Int, ts: Long, sid: Long, attrs: Seq[String] = allAttributes) =
    Try {
      logger.debug(s"event=get_session pk=$appId ts=$ts sid=$sid attrs=$attrs")
      val pk = appId
      val sk = getSortKey(ts, sid)
      val spec = new GetItemSpec()
        .withPrimaryKey("aid", pk, "sid", sk)
        .withAttributesToGet(attrs: _*)
      packSession(appId, ts, sid, attrs, table.getItem(spec))
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
      packSession(pk, ts, sid, attrs, item)
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
        packSession(pk, ts, sid, attrs, item)
      }
    }
  }
}