package ink.baixin.ripple.query.tables

import scala.util.Try
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.linq4j.AbstractEnumerable
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.models.Session
import ink.baixin.ripple.query.utils.RippleEnumerator
import ink.baixin.ripple.query.utils.RippleBufferSortEnumerator

class EventScanTable(private val sp: StateProvider) extends RippleTable {
  private val logger = Logger(this.getClass)

  private lazy val table = sp.resolver.getFactTable.get

  private val postMatcher = "postDetail\\?postId=\\d+".r.unanchored
  private val productMatcher = "productDetail\\?productId=\\d+".r.unanchored

  override val fields = Seq(
    ("app_id", SqlTypeName.INTEGER),
    ("open_id", SqlTypeName.VARCHAR),
    ("session_id", SqlTypeName.BIGINT),
    ("dwell_time", SqlTypeName.INTEGER),
    ("type", SqlTypeName.VARCHAR),
    ("subtype", SqlTypeName.VARCHAR),
    ("parameter", SqlTypeName.BIGINT),
    ("extra_parameter", SqlTypeName.VARCHAR),
    ("repeat_times", SqlTypeName.INTEGER),
    ("category", SqlTypeName.VARCHAR),
    ("cst_date", SqlTypeName.DATE),
    ("timestamp", SqlTypeName.TIMESTAMP)
  )

  override val nullableFields =
    Set("subtype", "parameter", "extra_parameter", "category")

  override def getAttrs(fields: Seq[String]) = {
    if (fields.contains("open_id")) {
      Seq("oid", "evs")
    } else {
      Seq("evs")
    }
  }


  private def getFieldValue(session: Session, event: Session.Event, field: String): AnyRef =
    field match {
      case "app_id" =>
        session.appId.asInstanceOf[java.lang.Integer]
      case "open_id" =>
        session.openId
      case "session_id" =>
        session.sessionId.asInstanceOf[java.lang.Long]
      case "dwell_time" =>
        event.dwellTime.asInstanceOf[java.lang.Integer]
      case "type" =>
        event.`type`
      case "subtype" =>
        event.subType
      case "parameter" =>
        val num = Try(event.parameter.toLong.asInstanceOf[java.lang.Long])
        if (num.isSuccess) num.get
        else null
      case "extra_parameter" =>
        event.extraParameter
      case "repeat_times" =>
        event.repeatTimes.asInstanceOf[java.lang.Integer]
      case "category" =>
        getCategory(event)
      case "cst_date" =>
        val hr = (session.timestamp / 3600 / 1000).toInt
        ((hr + 8) / 24).asInstanceOf[java.lang.Integer]
      case "timestamp"  =>
        event.timestamp.asInstanceOf[java.lang.Long]
    }

  private def getCategory(event: Session.Event) = {
    (event.`type`, event.subType) match {
      case ("pv", postMatcher(_*)) => "post"
      case ("pv", productMatcher(_*)) => "product"
      case _ => null
    }
  }

  private def performScan(ctx: DataContext, fields: Seq[String])(func: => Iterator[Session]) = {
    val cancelFlag: AtomicBoolean = DataContext.Variable.CANCEL_FLAG.get(ctx)
    new AbstractEnumerable[AnyRef]() {
      override def enumerator(): Enumerator[AnyRef] =
        RippleEnumerator[(Session, Session.Event)](cancelFlag, func.flatMap(s => s.events.map(e => (s, e)))) {
          (tuple) =>
            val (session, event) = tuple
            fields.map((f) => getFieldValue(session, event, f)).toArray
        }
    }
  }

  private def performScan(ctx: DataContext, fields: Seq[String], desc: Boolean)(func: => Iterator[Session]) = {
    val cancelFlag: AtomicBoolean = DataContext.Variable.CANCEL_FLAG.get(ctx)
    new AbstractEnumerable[AnyRef]() {
      override def enumerator(): Enumerator[AnyRef] =
        new RippleBufferSortEnumerator[(Session, Session.Event)](
          cancelFlag, 2000, func.flatMap(s => s.events.map(e => (s, e)))
        ) {
          override def transform(tuple: (Session, Session.Event)) = {
            val (session, event) = tuple
            fields.map((f) => getFieldValue(session, event, f)).toArray
          }

          override def compare(a: (Session, Session.Event), b: (Session, Session.Event)) = {
            if (desc) (b._2.timestamp - a._2.timestamp).toInt
            else (a._2.timestamp - b._2.timestamp).toInt
          }
        }
    }
  }

  override def getRowCount = 10E6

  override def scan(ctx: DataContext, predicate: RippleTable.Predicate) = {
    val fields = predicate.fields match {
      case Some(f) => f
      case None => fieldKeys
    }
    val attrs = getAttrs(fields)

    val (ts1, ts2) = predicate.timeRange match {
      case Some((a, b)) => (math.max(a - 600000L, 0L), math.max(a, b))
      case None => (0L, Long.MaxValue)
    }

    predicate match {
      case RippleTable.Predicate(Some(appId), _, Some(openIds), _, desc) =>
        val order = desc.getOrElse(true)
        logger.info(s"event=query_sessions app_id=$appId ts=${(ts1, ts2)} open_ids=$openIds attrs=$attrs desc=$order")
        performScan(ctx, fields, order)(table.querySessionsWithOpenIds(appId, ts1, ts2, openIds, attrs, order))
      case RippleTable.Predicate(Some(appId), _, _, _, desc) =>
        val order = desc.getOrElse(true)
        logger.info(s"event=query_sessions app_id=$appId ts=${(ts1, ts2)} attrs=$attrs desc=$order")
        performScan(ctx, fields, order)(table.querySessions(appId, ts1, ts2, attrs, order))
      case _ =>
        logger.warn(s"event=native_scan_sessions attrs=$attrs")
        performScan(ctx, fields)(table.scan(attrs))
    }
  }
}