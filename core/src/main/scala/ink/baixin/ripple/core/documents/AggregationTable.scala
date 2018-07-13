package ink.baixin.ripple.core.documents

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec

import com.typesafe.scalalogging.Logger

import scala.util.{ Try, Success, Failure }

import ink.baixin.ripple.core.models._

class AggregationTable(private val table: Table) {
  private val logger = Logger(this.getClass)

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

  private def packAggregations(aggs: Seq[AggregationRecord]) =
    AggregationPack(
      aggs.map(_.cstDate),
      aggs.map(_.sumDuration),
      aggs.map(_.sumPageviews),
      aggs.map(_.sumSharings),
      aggs.map(_.sumLikes),
      aggs.map(_.sumTotal),
      aggs.map(_.countSession),
      aggs.map(_.maxTimestamp)
    )

  private def unpackAggregations(appId: Int, openId: String, pack: AggregationPack) = {
    val length = pack.cstDate.length
    for (i <- 0 until length) yield {
      AggregationRecord(
        appId,
        openId,
        pack.cstDate(i),
        pack.sumDuration(i),
        pack.sumPageviews(i),
        pack.sumSharings(i),
        pack.sumLikes(i),
        pack.sumTotal(i),
        pack.countSession(i),
        pack.maxTimestamp(i)
      )
    }
  }

  def getAggregations(appId: Int, openId: String) = {
    val spec = new GetItemSpec()
      .withPrimaryKey("aid", appId, "oid", openId)
      .withAttributesToGet("agg")

    val item = table.getItem(spec)

    if (item == null) {
      Seq()
    } else {
      unpackAggregations(appId, openId, AggregationPack.parseFrom(item.getBinary("agg")))
    }
  }

  def putAggregations(appId: Int, openId: String, aggs: Seq[AggregationRecord], replace: Boolean = false) {
    logger.debug(s"event=put_aggregations app_id=${appId} open_id=$openId size=${aggs.size}")

    val oldAggregations = if (replace) Seq() else getAggregations(appId, openId)
    val oldAggMap = oldAggregations.map(agg => (agg.cstDate, agg)).toMap
    val newAggMap = aggs.map(agg => (agg.cstDate, agg)).toMap
    val allAggs = (oldAggMap ++ newAggMap).values.toSeq.sortBy(_.cstDate)

    val date = allAggs.last.cstDate

    val item = (new Item())
      .withPrimaryKey(
        "aid", appId,
        "oid", openId
      )
      .withBinary("agg", packAggregations(allAggs.filter(_.cstDate > date - 100)).toByteArray)
      .withInt("expire", (date + 100) * 24 * 3600) // expire after about 100 days

    table.putItem(item)
  }

  def queryAggregations(appId: Int, startDt: Int, endDt:Int) = {
    logger.debug(s"event=query_aggregations app_id=${appId} start=${startDt} end=${endDt}")
    val qs = new QuerySpec()
      .withHashKey("aid", appId)
      .withAttributesToGet("oid", "agg")

    safeQuery(qs).flatMap {
      (item) =>
        val openId = item.getString("oid")
        val pack = AggregationPack.parseFrom(item.getBinary("agg"))
        unpackAggregations(appId, openId, pack).filter(a => startDt <= a.cstDate && a.cstDate <= endDt)
    }
  }

  def queryAggregationsWithOpenIds(appId: Int, openIds: Set[String], startDt: Int, endDt: Int) = {
    logger.debug(s"event=query_aggregations_with_openids app_id=${appId} openid=${openIds.size} start=${startDt} end=${endDt}")
    val qs = new QuerySpec()
      .withHashKey("aid", appId)
      .withRangeKeyCondition(new RangeKeyCondition("oid").between(openIds.min, openIds.max))
      .withAttributesToGet("oid", "agg")

    safeQuery(qs).flatMap {
      (item) =>
        val openId = item.getString("oid")
        val pack = AggregationPack.parseFrom(item.getBinary("agg"))
        if (openIds.contains(openId)) {
          unpackAggregations(appId, openId, pack).filter(a => startDt <= a.cstDate && a.cstDate <= endDt)
        } else {
          Seq()
        }
    }
  }

  def scan() = {
    logger.debug(s"event=scan_aggregations")
    val spec = new ScanSpec()
      .withAttributesToGet("aid", "oid", "agg")

    safeScan(spec).flatMap {
      (item) =>
        val appId = item.getInt("aid")
        val openId = item.getString("oid")
        val pack = AggregationPack.parseFrom(item.getBinary("agg"))
        unpackAggregations(appId, openId, pack)
    }
  }
}