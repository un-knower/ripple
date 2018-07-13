package ink.baixin.ripple.query.tables

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.linq4j.AbstractEnumerable
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.models.Session
import ink.baixin.ripple.query.utils.RippleEnumerator

class SessionScanTable(private val sp: StateProvider) extends RippleTable {
  val logger = Logger(this.getClass)

  private lazy val table = sp.resolver.getFactTable.get

  override val fields = Seq(
    ("id", SqlTypeName.BIGINT),
    ("app_id", SqlTypeName.INTEGER),
    ("open_id", SqlTypeName.VARCHAR),
    ("scene", SqlTypeName.INTEGER),
    ("referrer", SqlTypeName.VARCHAR),
    ("duration", SqlTypeName.INTEGER),
    ("pageviews", SqlTypeName.INTEGER),
    ("sharings", SqlTypeName.INTEGER),
    ("likes", SqlTypeName.INTEGER),
    ("total", SqlTypeName.INTEGER),
    ("longitude", SqlTypeName.DOUBLE),
    ("latitude", SqlTypeName.DOUBLE),
    ("accuracy", SqlTypeName.DOUBLE),
    ("cst_date", SqlTypeName.DATE),
    ("timestamp", SqlTypeName.TIMESTAMP)
  )
  override val fieldGroups = Map(
    "oid" -> Set("open_id"),
    "ent" -> Set("scene", "referrer"),
    "agg" -> Set("duration", "pageviews", "sharings", "likes", "total"),
    "gps" -> Set("longitude", "latitude", "acccuracy")
  )

  override val nullableFields =
    Set("scene", "referrer", "longitude", "latitude", "accuracy")

  private def getFieldValue(session: Session, field: String): AnyRef =
    field match {
      case "id" =>
        session.sessionId.asInstanceOf[java.lang.Long]
      case "app_id" =>
        session.appId.asInstanceOf[java.lang.Integer]
      case "open_id" =>
        session.openId
      case "scene" =>
        session.entrance match {
          case Some(ent) => ent.scene.asInstanceOf[java.lang.Integer]
          case _ => null
        }
      case "referrer" =>
        session.entrance match {
          case Some(ent) => ent.referrer
          case _ => null
        }
      case "duration" =>
        session.aggregation match {
          case Some(agg) => agg.duration.asInstanceOf[java.lang.Integer]
          case _ => null
        }
      case "pageviews" =>
        session.aggregation match {
          case Some(agg) => agg.pageviews.asInstanceOf[java.lang.Integer]
          case _ => null
        }
      case "sharings" =>
        session.aggregation match {
          case Some(agg) => agg.sharings.asInstanceOf[java.lang.Integer]
          case _ => null
        }
      case "likes" =>
        session.aggregation match {
          case Some(agg) => agg.likes.asInstanceOf[java.lang.Integer]
          case _ => null
        }
      case "total" =>
        session.aggregation match {
          case Some(agg) => agg.total.asInstanceOf[java.lang.Integer]
          case _ => null
        }
      case "longitude" =>
        session.gpsLocation match {
          case Some(gps) => gps.longitude.asInstanceOf[java.lang.Double]
          case _ => null
        }
      case "latitude" =>
        session.gpsLocation match {
          case Some(gps) => gps.latitude.asInstanceOf[java.lang.Double]
          case _ => null
        }
      case "accuracy" =>
        session.gpsLocation match {
          case Some(gps) => gps.locationAccuracy.asInstanceOf[java.lang.Double]
          case _ => null
        }
      case "cst_date"  =>
        val hr = (session.timestamp / 3600 / 1000).toInt
        ((hr + 8) / 24).asInstanceOf[java.lang.Integer]
      case "timestamp"  =>
        session.timestamp.asInstanceOf[java.lang.Long]
    }

  private def performScan(ctx: DataContext, fields: Seq[String])(func: => Iterator[Session]) = {
    val cancelFlag: AtomicBoolean = DataContext.Variable.CANCEL_FLAG.get(ctx)
    new AbstractEnumerable[AnyRef]() {
      override def enumerator(): Enumerator[AnyRef] =
        RippleEnumerator[Session](cancelFlag, func) {
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
    val attrs = getAttrs(fields)

    val (ts1, ts2) = predicate.timeRange match {
      case Some(tr) => tr
      case None => (0L, Long.MaxValue)
    }

    predicate match {
      case RippleTable.Predicate(Some(appId), _, Some(openIds), _, desc) =>
        logger.info(s"event=query_sessions app_id=$appId ts=${(ts1, ts2)} open_ids=$openIds attrs=$attrs")
        performScan(ctx, fields)(table.querySessionsWithOpenIds(appId, ts1, ts2, openIds, attrs, desc.getOrElse(true)))
      case RippleTable.Predicate(Some(appId), _, _, _, desc) =>
        logger.info(s"event=query_sessions app_id=$appId ts=${(ts1, ts2)} attrs=$attrs")
        performScan(ctx, fields)(table.querySessions(appId, ts1, ts2, attrs, desc.getOrElse(true)))
      case _ =>
        logger.warn(s"event=native_scan_sessions attrs=$attrs")
        performScan(ctx, fields)(table.scan(attrs))
    }
  }
}