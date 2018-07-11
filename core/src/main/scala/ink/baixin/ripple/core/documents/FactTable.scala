package ink.baixin.ripple.core.documents

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.PrimaryKey
import com.amazonaws.services.dynamodbv2.document.QueryFilter
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec
import com.typesafe.scalalogging.Logger
import scala.util.{ Try, Success, Failure }
import ink.baixin.ripple.core.models._

class FactTable(private val table: Table) {
  private val logger = Logger(this.getClass)

  private def getSortKey(timestamp: Long, sessionId: Long) = {
    val bf = java.nio.ByteBuffer.allocate(16)
    bf.putLong(timestamp)
    bf.putLong(sessionId)
    bf.array
  }

  private def parseSortKey(sid: Array[Byte]) = {
    val bf = java.nio.ByteBuffer.wrap(sid)
    val timestamp = bf.getLong
    val sessionId = bf.getLong
    (timestamp, sessionId)
  }

  private def safeQuery(spec: QuerySpec) =
    Try(table.query(spec).iterator()) match {
      case Success(iter) =>
        new Iterator[Item]() {
          override def hasNext() = iter.hasNext()
          override def next() = iter.next()
        }
      case Failure(e) =>
        logger.error(s"event=safe_query error=$e")
        Seq[Item]().iterator
    }

  private def safeScan(spec: ScanSpec) =
    Try(table.scan(spec).iterator()) match {
      case Success(iter) =>
        new Iterator[Item]() {
          override def hasNext() = iter.hasNext()
          override def next() = iter.next()
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
          evs.map(_.extraParameter),
          evs.map(_.repeatTimes)
        ).toByteArray
      case _ => new Array[Byte](0)
    }
  }

  private def packSession(pk:Int, ts: Long, sid: Long, attrs: Seq[String], item: Item) =
    attrs.foldLeft(
      Session().withAppId(pk).withTimestamp(ts).withSessionId(sid)
    ) {
      case (s, "oid") if item.hasAttribute("oid") =>
        s.withOpenId(item.getString("oid"))
      case (s, "ent") if item.hasAttribute("ent") =>
        s.withEntrance(Session.Entrance.parseFrom(item.getBinary("ent")))
      case (s, "agg") if item.hasAttribute("agg") =>
        s.withAggregation(Session.Aggregation.parseFrom(item.getBinary("agg")))
      case (s, "gps") if item.hasAttribute("gps") =>
        s.withGpsLocation(Session.GPSLocation.parseFrom(item.getBinary("gps")))
      case (s, "evs") if item.hasAttribute("evs") =>
        val eventsPack = Session.EventsPack.parseFrom(item.getBinary("evs"))
        val events = for (i <- 0 until eventsPack.timestamp.size) yield {
          Session.Event(
            eventsPack.timestamp(i),
            eventsPack.dwellTime.lift(i).getOrElse(0),
            eventsPack.`type`(i),
            eventsPack.subType(i),
            eventsPack.parameter.lift(i).getOrElse(""),
            eventsPack.extraParameter.lift(i).getOrElse(""),
            eventsPack.repeatTimes.lift(i).getOrElse(0)
          )
        }
        s.withEvents(events)
      case (s, _) => s
    }

  private def mergeSortIters(iters: Seq[Iterator[Session]], descending: Boolean) = {
    if (iters.length == 1) iters(0)
    else {
      logger.debug(s"event=merge_sort_iters iters=${iters}")
      val ord = new Ordering[(Session, Int)] {
        override def compare(a: (Session, Int), b: (Session, Int)) = {
          if (descending) {
            (a._1.timestamp compare b._1.timestamp)
          } else {
            -(a._1.timestamp compare b._1.timestamp)
          }
        }
      }
      new Iterator[Session]() {
        val pq = {
          val initial = iters.zipWithIndex.collect {
            case (iter, idx) if iter.hasNext => (iter.next, idx)
          }
          val q = new scala.collection.mutable.PriorityQueue[(Session, Int)]()(ord)
          q.enqueue(initial: _*)
          q
        }
        override def hasNext() = !pq.isEmpty
        override def next() = {
          val (res, idx) = pq.dequeue
          if (iters(idx).hasNext) pq.enqueue((iters(idx).next, idx))
          res
        }
      }
    }
  }

  val allAttributes = Seq("oid", "ent", "agg", "evs", "gps")

  def putSession(session: Session) = Try {
    // check parameters
    assert(!session.openId.isEmpty)
    assert(!session.events.isEmpty)
    logger.debug(s"event=put_session app_id=${session.appId} ts=${session.timestamp} sid=${session.sessionId} openid=${session.openId}")

    val keyItem = (new Item())
      .withPrimaryKey("aid", session.appId, "sid", getSortKey(session.timestamp, session.sessionId))
      .withString("oid", session.openId)

    val item = Seq(
      ("ent", packField(session.entrance)),
      ("agg", packField(session.aggregation)),
      ("gps", packField(session.gpsLocation)),
      ("evs", packField(session.events))
    ).foldLeft(keyItem) {
      case (it, (attr, value)) if !value.isEmpty => it.withBinary(attr, value)
      case (it, _)  => it
    }

    table.putItem(
      item.withNumber("expire", session.timestamp / 1000 + 20 * 24 * 3600) // expire the item after 20 days
    )
  }

  def deleteSession(appId: Int, ts: Long, sid: Long) = Try {
    table.deleteItem(new PrimaryKey("aid", appId, "sid", getSortKey(ts, sid)))
  }

  def getSession(appId: Int, ts: Long, sid: Long, attrs: Seq[String] = allAttributes) = Try {
    logger.debug(s"event=get_session pk=$appId ts=$ts sid=$sid attrs=$attrs")
    val pk = appId
    val sk = getSortKey(ts, sid)

    val spec = new GetItemSpec()
      .withPrimaryKey("aid", pk, "sid", sk)
      .withAttributesToGet(attrs: _*)

    packSession(appId, ts, sid, attrs, table.getItem(spec))
  }

  def querySessions(appId: Int, start_ts: Long, end_ts: Long, attrs: Seq[String] = allAttributes,
                    descending: Boolean = true
                   ) = {
    logger.debug(s"event=query_sessions pk=$appId start=$start_ts end=$end_ts attrs=$attrs")
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    val spec = new QuerySpec()
      .withHashKey("aid", pk)
      .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
      .withAttributesToGet((attrs :+ "sid"): _*)
      .withScanIndexForward(!descending)

    safeQuery(spec).map {
      (item) =>
        val (ts, sid) = parseSortKey(item.getBinary("sid"))
        packSession(pk, ts, sid, attrs, item)
    }
  }

  def querySessionsWithOpenIds(
                                appId: Int, start_ts: Long, end_ts: Long, openIds: Set[String], attrs: Seq[String] = allAttributes,
                                descending: Boolean = true
                              ) = {
    logger.debug(s"event=query_sessions_with_openids pk=$appId start=$start_ts end=$end_ts openids=$openIds attrs=$attrs")
    val pk = appId
    val ssk = getSortKey(start_ts, 0)
    val esk = getSortKey(end_ts, -1)

    val iters = openIds.iterator.sliding(100, 100).map { (oids) =>
      val spec = new QuerySpec()
        .withHashKey("aid", pk)
        .withRangeKeyCondition(new RangeKeyCondition("sid").between(ssk, esk))
        .withAttributesToGet((attrs :+ "sid"): _*)
        .withQueryFilters(new QueryFilter("oid").in(oids.toSeq: _*))
        .withScanIndexForward(!descending)

      safeQuery(spec).map {
        (item) =>
          val (ts, sid) = parseSortKey(item.getBinary("sid"))
          packSession(pk, ts, sid, attrs, item)
      }
    }

    mergeSortIters(iters.toSeq, descending)
  }

  def scan(attrs: Seq[String] = allAttributes) = {
    logger.debug(s"event=scan_sessions attrs=$attrs")
    val spec = new ScanSpec()
      .withAttributesToGet((Seq("aid", "sid") ++ attrs): _*)

    safeScan(spec).map {
      (item) =>
        val pk = item.getInt("aid")
        val (ts, sid) = parseSortKey(item.getBinary("sid"))
        packSession(pk, ts, sid, attrs, item)
    }
  }
}