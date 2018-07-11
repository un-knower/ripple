package ink.baixin.ripple.core.documents

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.typesafe.scalalogging.Logger
import scala.util.{ Try, Success, Failure }
import ink.baixin.ripple.core.models._

object AggregationTable {
  val AH_GROUP = (1 << 0) // appId, hour
  val AOD_GROUP = (1 << 1) // appId, openId, date
}

class AggregationTable(private val table: Table) {
  private val logger = Logger(this.getClass)

  private def getPrimaryKey(aggGroup: Int, appId: Int) = (aggGroup.toLong << 32 | appId.toLong)
  private def unpackPrimaryKey(pk: Long) = ((pk >> 32).toInt, pk.toInt)

  private def getSortKey(hour: Int) =
    String.format("%08X", hour.asInstanceOf[java.lang.Integer])

  private def getSortKey(openId: String, date: Int) =
    String.format("%-28s|%08X", openId, date.asInstanceOf[java.lang.Integer])

  private def unpackSortKey(rid: String) = {
    rid.split("\\|") match {
      case Array(hourstr) => (None, Some(Integer.parseInt(hourstr, 16)))
      case Array(openId, dateStr) => (Some(openId.trim), Some(Integer.parseInt(dateStr, 16)))
      case _ => (None, None)
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

  private def packAggregations(group: Int, aggs: Seq[AggregationRecord]) =
    group match {
      case AggregationTable.AH_GROUP =>
        AggregationPack(
          Seq[Int](),
          aggs.map(_.openId),
          aggs.map(_.sumDuration),
          aggs.map(_.sumPageviews),
          aggs.map(_.sumSharings),
          aggs.map(_.sumLikes),
          aggs.map(_.sumTotal),
          aggs.map(_.countSession),
          aggs.map(_.maxStarttime)
        )
      case AggregationTable.AOD_GROUP =>
        AggregationPack(
          aggs.map(_.timeHour),
          Seq[String](),
          aggs.map(_.sumDuration),
          aggs.map(_.sumPageviews),
          aggs.map(_.sumSharings),
          aggs.map(_.sumLikes),
          aggs.map(_.sumTotal),
          aggs.map(_.countSession),
          aggs.map(_.maxStarttime)
        )
    }

  private def unpackAggregations(appId: Int, timeHour: Int, pack: AggregationPack) = {
    val length = Seq(
      pack.timeHour.length,
      pack.openId.length,
      pack.sumDuration.length,
      pack.sumPageviews.length,
      pack.sumSharings.length,
      pack.sumSharings.length,
      pack.sumLikes.length,
      pack.sumTotal.length,
      pack.countSession.length,
      pack.maxStarttime.length
    ).max

    def getItemOrElse[T](arr: Seq[T], index: Int, default: T) =
      if (index < arr.length) arr(index) else default

    for (i <- 0 until length) yield {
      AggregationRecord(
        appId,
        timeHour,
        getItemOrElse(pack.openId, i, ""),
        getItemOrElse(pack.sumDuration, i, 0),
        getItemOrElse(pack.sumPageviews, i, 0),
        getItemOrElse(pack.sumSharings, i, 0),
        getItemOrElse(pack.sumLikes, i, 0),
        getItemOrElse(pack.sumTotal, i, 0),
        getItemOrElse(pack.countSession, i, 0),
        getItemOrElse(pack.maxStarttime, i, 0)
      )
    }
  }

  private def unpackAggregations(appId: Int, openId: String, pack: AggregationPack) = {
    val length = Seq(
      pack.timeHour.length,
      pack.openId.length,
      pack.sumDuration.length,
      pack.sumPageviews.length,
      pack.sumSharings.length,
      pack.sumSharings.length,
      pack.sumLikes.length,
      pack.sumTotal.length,
      pack.countSession.length,
      pack.maxStarttime.length
    ).max

    def getItemOrElse[T](arr: Seq[T], index: Int, default: T) =
      if (index < arr.length) arr(index) else default

    for (i <- 0 until length) yield {
      AggregationRecord(
        appId,
        getItemOrElse(pack.timeHour, i, 0),
        openId,
        getItemOrElse(pack.sumDuration, i, 0),
        getItemOrElse(pack.sumPageviews, i, 0),
        getItemOrElse(pack.sumSharings, i, 0),
        getItemOrElse(pack.sumLikes, i, 0),
        getItemOrElse(pack.sumTotal, i, 0),
        getItemOrElse(pack.countSession, i, 0),
        getItemOrElse(pack.maxStarttime, i, 0)
      )
    }
  }

  def putAggregations(appId: Int, hour: Int, aggs: Seq[AggregationRecord]) {
    logger.debug(s"event=put_aggregation app_id=${appId} hour=${hour} size=${aggs.size}")

    val item = (new Item())
      .withPrimaryKey(
        "pk", getPrimaryKey(AggregationTable.AH_GROUP, appId),
        "rid", getSortKey(hour)
      )
      .withBinary("agg", packAggregations(AggregationTable.AH_GROUP, aggs).toByteArray)
      .withInt("expire", (hour + 24 * 100) * 3600) // expire after about 100 days

    table.putItem(item)
  }

  def putAggregations(appId: Int, openId: String, date: Int, aggs: Seq[AggregationRecord]) {
    logger.debug(s"event=put_aggregation app_id=${appId} open_id=$openId date=${date} size=${aggs.size}")

    val item = (new Item())
      .withPrimaryKey(
        "pk", getPrimaryKey(AggregationTable.AOD_GROUP, appId),
        "rid", getSortKey(openId, date)
      )
      .withBinary("agg", packAggregations(AggregationTable.AOD_GROUP, aggs).toByteArray)
      .withInt("expire", (date + 100) * 24 * 3600) // expire after about 100 days

    table.putItem(item)
  }

  def queryAggregations(appId: Int, startHour: Int, endHour:Int, descending: Boolean) = {
    logger.debug(s"event=query_aggregations app_id=${appId} start=${startHour} end=${endHour}")
    val qs = new QuerySpec()
      .withHashKey("pk", getPrimaryKey(AggregationTable.AH_GROUP, appId))
      .withRangeKeyCondition(
        new RangeKeyCondition("rid").between(getSortKey(startHour), getSortKey(endHour))
      )
      .withAttributesToGet("rid", "agg")
      .withScanIndexForward(!descending)

    safeQuery(qs).flatMap {
      (item) =>
        val pack = AggregationPack.parseFrom(item.getBinary("agg"))
        unpackSortKey(item.getString("rid")) match {
          case (None, Some(hour)) => unpackAggregations(appId, hour, pack)
          case _ => Seq()
        }
    }
  }

  def queryAggregationsWithOpenIds(appId: Int, openIds: Set[String], startHour: Int, endHour: Int, descending: Boolean) = {
    logger.debug(s"event=query_aggregations_with_openids app_id=${appId} openid=${openIds.size} start=${startHour} end=${endHour}")
    val qs = new QuerySpec()
      .withHashKey("pk", getPrimaryKey(AggregationTable.AOD_GROUP, appId))
      .withRangeKeyCondition(
        new RangeKeyCondition("rid").between(
          getSortKey(openIds.min, startHour / 24), getSortKey(openIds.max, endHour / 24)
        )
      )
      .withAttributesToGet("rid", "agg")
      .withScanIndexForward(!descending)

    safeQuery(qs).flatMap {
      (item) =>
        val pack = AggregationPack.parseFrom(item.getBinary("agg"))
        unpackSortKey(item.getString("rid")) match {
          case (Some(openId), _) if openIds.contains(openId) =>
            unpackAggregations(appId, openId, pack)
              .filter(rec => rec.timeHour >= startHour && rec.timeHour <= endHour)
          case _ => Seq()
        }
    }
  }

  def scan() = {
    logger.debug(s"event=scan_aggregations")
    val spec = new ScanSpec()
      .withAttributesToGet("pk", "rid", "agg")

    safeScan(spec).flatMap {
      (item) =>
        val pack = AggregationPack.parseFrom(item.getBinary("agg"))
        unpackPrimaryKey(item.getLong("pk")) match {
          case (AggregationTable.AH_GROUP, appId) =>
            unpackSortKey(item.getString("rid")) match {
              case (None, Some(hour)) => unpackAggregations(appId, hour, pack)
              case _ => Seq()
            }
          case (AggregationTable.AOD_GROUP, appId) =>
            unpackSortKey(item.getString("rid")) match {
              case (Some(openId), _) => unpackAggregations(appId, openId, pack)
              case _ => Seq()
            }
          case _ => Seq()
        }
    }
  }
}