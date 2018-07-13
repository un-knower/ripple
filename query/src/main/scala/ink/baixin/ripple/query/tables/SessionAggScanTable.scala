package ink.baixin.ripple.query.tables

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.linq4j.AbstractEnumerable
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.models.Session
import ink.baixin.ripple.core.models.AggregationRecord
import ink.baixin.ripple.query.utils._

class SessionAggScanTable(private val sp: StateProvider) extends RippleTable {
  private val logger = Logger(this.getClass)
  private lazy val factTable = sp.resolver.getFactTable.get
  private lazy val aggTable = sp.resolver.getAggregationTable.get

  private def getCSTDate(ts: Long) = (((ts / 3600 / 1000) + 8) / 24).toInt
  private def session2Aggregation(session: Session) = {
    AggregationRecord(
      session.appId,
      session.openId,
      getCSTDate(session.timestamp),
      session.getAggregation.duration,
      session.getAggregation.pageviews,
      session.getAggregation.sharings,
      session.getAggregation.likes,
      session.getAggregation.total,
      1,
      session.timestamp
    )
  }

  override val fields = Seq(
    ("app_id", SqlTypeName.INTEGER),
    ("open_id", SqlTypeName.VARCHAR),
    ("cst_date", SqlTypeName.DATE),
    ("sum_duration", SqlTypeName.INTEGER),
    ("sum_pageviews", SqlTypeName.INTEGER),
    ("sum_sharings", SqlTypeName.INTEGER),
    ("sum_likes", SqlTypeName.INTEGER),
    ("sum_total", SqlTypeName.INTEGER),
    ("count_session", SqlTypeName.INTEGER),
    ("max_timestamp", SqlTypeName.TIMESTAMP)
  )

  override val nullableFields = Set()
  override def getAttrs(fields: Seq[String]) = Seq("aid", "oid", "agg")

  private def getFieldValue(agg: AggregationRecord, field: String): AnyRef =
    field match {
      case "app_id" =>
        agg.appId.asInstanceOf[java.lang.Integer]
      case "open_id" =>
        agg.openId
      case "cst_date" =>
        agg.cstDate.asInstanceOf[java.lang.Integer]
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
      case "max_timestamp" =>
        agg.maxTimestamp.asInstanceOf[java.lang.Long]
    }

  private def performScan(ctx: DataContext, fields: Seq[String])(func: => Iterator[AggregationRecord]) = {
    val cancelFlag: AtomicBoolean = DataContext.Variable.CANCEL_FLAG.get(ctx)
    new AbstractEnumerable[AnyRef]() {
      override def enumerator(): Enumerator[AnyRef] =
        RippleEnumerator[AggregationRecord](cancelFlag, func) {
          (agg) =>
            fields.map((f) => getFieldValue(agg, f)).toArray
        }
    }
  }

  override def getRowCount = 10E5

  override def scan(ctx: DataContext, predicate: RippleTable.Predicate) = {
    val fields = predicate.fields match {
      case Some(f) => f
      case None => fieldKeys
    }

    val watermark = sp.listener.getState match {
      case Some(state) => state.watermark
      case None =>
        logger.warn(s"event=fail_to_get_watermark")
        0L
    }

    if (watermark < System.currentTimeMillis - 15L * 24L * 60L * 60L * 1000L) {
      // watermark is less than maximum available interval of realtime data
      logger.warn(s"event=watermark_too_less watermark=$watermark")
    }

    val (ts1, ts2) = predicate.timeRange match {
      case Some((a, b)) => (math.max(a, watermark), math.max(b, watermark))
      case None => (watermark, Long.MaxValue)
    }

    val (dt1, dt2) = predicate.timeRange match {
      case Some((a, b)) =>
        val wmDt = getCSTDate(watermark - 1)
        (math.min(getCSTDate(a), wmDt), math.min(getCSTDate(b), wmDt))
      case None => (getCSTDate(watermark - 1), Int.MaxValue)
    }

    predicate match {
      case RippleTable.Predicate(Some(appId), _, Some(openIds), _, _) =>
        logger.info(s"event=query_aggregations app_id=$appId dt=${(dt1, dt2)} wm=$watermark ts=${(ts1, ts2)} open_ids=$openIds")
        val aggRes = aggTable.queryAggregationsWithOpenIds(appId, openIds, dt1, dt2)
        val sessRes = factTable
          .querySessionsWithOpenIds(appId, ts1, ts2, openIds, Seq("oid", "agg"))
          .map(session2Aggregation)
        performScan(ctx, fields)(aggRes ++ sessRes)
      case RippleTable.Predicate(Some(appId), _, _, _, _) =>
        logger.info(s"event=query_aggregations app_id=$appId dt=${(dt1, dt2)} wm=$watermark ts=${(ts1, ts2)}")
        val aggRes = aggTable.queryAggregations(appId, dt1, dt2)
        val sessRes = factTable
          .querySessions(appId, ts1, ts2, Seq("oid", "agg"))
          .map(session2Aggregation)
        performScan(ctx, fields)(aggRes ++ sessRes)
      case _ =>
        logger.warn(s"event=native_scan_aggregations")
        val aggRes = aggTable.scan().filter(agg => dt1 <= agg.cstDate && agg.cstDate <= dt2)
        val sessRes = factTable.scan(Seq("oid", "agg")).collect {
          case sess if ts1 <= sess.timestamp && sess.timestamp <= ts2 => session2Aggregation(sess)
        }
        performScan(ctx, fields)(aggRes ++ sessRes)
    }
  }
}