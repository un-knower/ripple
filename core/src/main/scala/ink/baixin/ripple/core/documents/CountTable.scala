package ink.baixin.ripple.core.documents

import com.amazonaws.services.dynamodbv2.model.ReturnValue
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.QueryFilter
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.typesafe.scalalogging.Logger
import scala.util.{ Try, Success, Failure }
import ink.baixin.ripple.core.models._

class CountTable(private val table: Table) {
  private val logger = Logger(this.getClass)

  private def getSortKey(openId: String, eventType: String, eventKey: String) = {
    s"$openId|$eventType|$eventKey"
  }

  private def parseSortKey(sid: String) = {
    sid.split("\\|", 3) match {
      case Array(openId, eventType, eventKey) => Some((openId, eventType, eventKey))
      case _ => None
    }
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

  val allAttributes = Seq("aid", "sid", "cnt", "ts")

  def increaseCount(appId: Int, openId: String, eventType: String, eventKey: String, timestamp: Long) = Try {
    logger.debug(s"event=increase_count pk=$appId open_id=$openId event_type=$eventType event_key=$eventKey ts=$timestamp")
    val updateSpec = new UpdateItemSpec()
      .withPrimaryKey("aid", appId, "sid", getSortKey(openId, eventType, eventKey))
      .withUpdateExpression("ADD cnt :val SET ts = :timestamp, expire = :expire")
      .withConditionExpression("attribute_not_exists(ts) or ts < :timestamp")
      .withValueMap(
        new ValueMap()
          .withNumber(":val", 1)
          .withNumber(":expire", (timestamp / 1000) + 3600 * 24 * 100) // keep a counter only for 100 days
          .withNumber(":timestamp", timestamp)
      )
      .withReturnValues(ReturnValue.ALL_NEW)

    val item = table.updateItem(updateSpec).getItem
    CountRecord(appId, openId, eventType, eventKey, item.getInt("cnt"), item.getLong("ts"))
  }

  def getCount(appId: Int, openId: String, eventType: String, eventKey: String, attrs: Seq[String] = allAttributes) = Try {
    logger.debug(s"event=get_count pk=$appId open_id=$openId event_type=$eventType event_key=$eventKey")
    val pk = appId
    val sk = getSortKey(openId, eventType, eventKey)

    val spec = new GetItemSpec()
      .withPrimaryKey("aid", pk, "sid", sk)
      .withAttributesToGet(attrs: _*)

    val item = table.getItem(spec)
    CountRecord(
      appId, openId, eventType, eventKey,
      if (attrs.contains("cnt")) item.getInt("cnt") else 0,
      if (attrs.contains("ts")) item.getLong("ts") else 0L
    )
  }

  def queryCounts(appId: Int, start_ts: Long, end_ts: Long) = {
    logger.debug(s"event=query_count pk=$appId start=$start_ts end=$end_ts")
    val spec = new QuerySpec()
      .withHashKey("aid", appId)
      .withQueryFilters(new QueryFilter("ts").between(start_ts, end_ts))
      .withAttributesToGet(allAttributes: _*)

    safeQuery(spec).flatMap {
      (item) =>
        parseSortKey(item.getString("sid")).collect {
          case (openId, eventType, eventKey) =>
            CountRecord(appId, openId, eventType, eventKey, item.getInt("cnt"), item.getLong("ts"))
        }
    }.filter(r => r.timestamp >= start_ts & r.timestamp <= end_ts)
  }

  def queryCountsWithOpenIds(appId: Int, start_ts: Long, end_ts: Long, openIds: Set[String]) =
    queryCounts(appId, start_ts, end_ts).filter(r => openIds.contains(r.openId))

  def scan() = {
    logger.debug(s"event=scan_counts")
    val spec = new ScanSpec()
      .withAttributesToGet(allAttributes: _*)

    safeScan(spec).flatMap {
      (item) =>
        val appId = item.getInt("aid")
        parseSortKey(item.getString("sid")).collect {
          case (openId, eventType, eventKey) =>
            CountRecord(appId, openId, eventType, eventKey, item.getInt("cnt"), item.getLong("ts"))
        }
    }
  }
}