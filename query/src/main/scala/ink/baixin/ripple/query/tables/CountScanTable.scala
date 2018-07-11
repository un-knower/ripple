package ink.baixin.ripple.query.tables

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.linq4j.AbstractEnumerable
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.models.CountRecord
import ink.baixin.ripple.query.utils.RippleEnumerator
import ink.baixin.ripple.query.utils.RippleBufferSortEnumerator

class CountScanTable(private val sp: StateProvider) extends RippleTable {
  private val logger = Logger(this.getClass)

  private lazy val table = sp.resolver.getCountTable.get

  override val fields = Seq(
    ("app_id", SqlTypeName.INTEGER),
    ("open_id", SqlTypeName.VARCHAR),
    ("event_type", SqlTypeName.VARCHAR),
    ("event_key", SqlTypeName.VARCHAR),
    ("count", SqlTypeName.INTEGER),
    ("timestamp", SqlTypeName.TIMESTAMP)
  )

  override val nullableFields = Set()
  override def getAttrs(fields: Seq[String]) = Seq("aid", "sid", "cnt", "ts")


  private def getFieldValue(cnt: CountRecord, field: String): AnyRef =
    field match {
      case "app_id" =>
        cnt.appId.asInstanceOf[java.lang.Integer]
      case "open_id" =>
        cnt.openId
      case "event_type" =>
        cnt.eventType
      case "event_key" =>
        cnt.eventKey
      case "count" =>
        cnt.count.asInstanceOf[java.lang.Integer]
      case "timestamp" =>
        cnt.timestamp.asInstanceOf[java.lang.Long]
    }

  private def performScan(ctx: DataContext, fields: Seq[String])(func: => Iterator[CountRecord]) = {
    val cancelFlag: AtomicBoolean = DataContext.Variable.CANCEL_FLAG.get(ctx)
    new AbstractEnumerable[AnyRef]() {
      override def enumerator(): Enumerator[AnyRef] =
        RippleEnumerator[CountRecord](cancelFlag, func) {
          (counts) =>
            fields.map((f) => getFieldValue(counts, f)).toArray
        }
    }
  }

  private def performScan(ctx: DataContext, fields: Seq[String], desc: Boolean)(func: => Iterator[CountRecord]) = {
    val cancelFlag: AtomicBoolean = DataContext.Variable.CANCEL_FLAG.get(ctx)
    new AbstractEnumerable[AnyRef]() {
      override def enumerator(): Enumerator[AnyRef] =
        new RippleBufferSortEnumerator[CountRecord](cancelFlag, 1000, func) {
          override def transform(record: CountRecord) = {
            fields.map((f) => getFieldValue(record, f)).toArray
          }

          override def compare(a: CountRecord, b: CountRecord) = {
            if (desc) (b.timestamp - a.timestamp).toInt
            else (a.timestamp - b.timestamp).toInt
          }
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
      case Some((a, b)) => (a, b)
      case None => (0L, Long.MaxValue)
    }

    predicate match {
      case RippleTable.Predicate(Some(appId), _, None, _, None) =>
        logger.info(s"event=query_counts app_id=$appId ts=${(ts1, ts2)}")
        performScan(ctx, fields)(table.queryCounts(appId, ts1, ts2))
      case RippleTable.Predicate(Some(appId), _, None, _, Some(desc)) =>
        logger.info(s"event=query_counts app_id=$appId ts=${(ts1, ts2)} descending=$desc")
        performScan(ctx, fields, desc)(table.queryCounts(appId, ts1, ts2))
      case RippleTable.Predicate(Some(appId), _, Some(openIds), _, None) =>
        logger.info(s"event=query_counts app_id=$appId ts=${(ts1, ts2)} open_ids=$openIds")
        performScan(ctx, fields)(table.queryCountsWithOpenIds(appId, ts1, ts2, openIds))
      case RippleTable.Predicate(Some(appId), _, Some(openIds), _, Some(desc)) =>
        logger.info(s"event=query_counts app_id=$appId ts=${(ts1, ts2)} open_ids=$openIds descending=$desc")
        performScan(ctx, fields, desc)(table.queryCountsWithOpenIds(appId, ts1, ts2, openIds))
      case _ =>
        logger.warn(s"event=native_scan_counts")
        performScan(ctx, fields)(table.scan())
    }
  }
}