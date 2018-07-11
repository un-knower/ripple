package ink.baixin.ripple.query.tables

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.linq4j.AbstractEnumerable
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.models.AggregationRecord
import ink.baixin.ripple.query.utils.RippleEnumerator

class AggregationScanTable(private val sp: StateProvider) extends RippleTable {
  private val logger = Logger(this.getClass)

  private lazy val table = sp.resolver.getAggregationTable.get

  override val fields = Seq(
    ("app_id", SqlTypeName.INTEGER),
    ("time_hour", SqlTypeName.TIMESTAMP),
    ("open_id", SqlTypeName.VARCHAR),
    ("sum_duration", SqlTypeName.INTEGER),
    ("sum_pageviews", SqlTypeName.INTEGER),
    ("sum_sharings", SqlTypeName.INTEGER),
    ("sum_likes", SqlTypeName.INTEGER),
    ("sum_total", SqlTypeName.INTEGER),
    ("count_session", SqlTypeName.INTEGER),
    ("max_starttime", SqlTypeName.TIMESTAMP),
  )

  override val nullableFields = Set()
  override def getAttrs(fields: Seq[String]) = Seq("pk", "rid", "agg")

  private def getFieldValue(agg: AggregationRecord, field: String): AnyRef =
    field match {
      case "app_id" =>
        agg.appId.asInstanceOf[java.lang.Integer]
      case "open_id" =>
        agg.openId
      case "time_hour" =>
        (agg.timeHour * 3600L * 1000L).asInstanceOf[java.lang.Long]
      case "sum_duration" =>
        agg.sumDuration.asInstanceOf[java.lang.Integer]
      case "sum_pageviews" =>
        agg.sumPageviews.asInstanceOf[java.lang.Integer]
      case "sum_sharings" =>
        agg.sumSharings.asInstanceOf[java.lang.Integer]
      case "sum_likes" =>
        agg.sumLikes.asInstanceOf[java.lang.Integer]
      case "sum_total" =>
        agg.sumTotal.asInstanceOf[java.lang.Integer]
      case "count_session" =>
        agg.countSession.asInstanceOf[java.lang.Integer]
      case "max_starttime" =>
        agg.maxStarttime.asInstanceOf[java.lang.Long]
    }

  private def performScan(ctx: DataContext, fields: Seq[String])(func: => Iterator[AggregationRecord]) = {
    val cancelFlag: AtomicBoolean = DataContext.Variable.CANCEL_FLAG.get(ctx)
    new AbstractEnumerable[AnyRef]() {
      override def enumerator(): Enumerator[AnyRef] =
        RippleEnumerator[AggregationRecord](cancelFlag, func) {
          (session) =>
            fields.map((f) => getFieldValue(session, f)).toArray
        }
    }
  }

  override def getRowCount = 10E5

  override def scan(ctx: DataContext, predicate: RippleTable.Predicate) = {
    val fields = predicate.fields match {
      case Some(f) => f
      case None => fieldKeys
    }
    val (ts1, ts2) = predicate.timeRange match {
      case Some((a, b)) => ((a / 3600000L).toInt, (b / 3600000L).toInt)
      case None => (0, Int.MaxValue)
    }

    predicate match {
      case RippleTable.Predicate(Some(appId), _, Some(openIds), _, desc) =>
        logger.info(s"event=query_aggregations app_id=$appId ts=${(ts1, ts2)} open_ids=$openIds")
        performScan(ctx, fields)(table.queryAggregationsWithOpenIds(appId, openIds, ts1, ts2, desc.getOrElse(true)))
      case RippleTable.Predicate(Some(appId), _, _, _, desc) =>
        logger.info(s"event=query_aggregations app_id=$appId ts=${(ts1, ts2)}")
        performScan(ctx, fields)(table.queryAggregations(appId, ts1, ts2, desc.getOrElse(true)))
      case _ =>
        logger.warn(s"event=native_scan_aggregations")
        performScan(ctx, fields)(table.scan())
    }
  }
}