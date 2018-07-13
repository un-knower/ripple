package ink.baixin.ripple.spark

import com.typesafe.scalalogging.Logger
import org.joda.time.{ DateTime, Days, Interval, DateTimeZone }
import org.apache.spark._
import org.apache.spark.rdd.RDD
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.models.{ Record, Session, AggregationRecord }

object RippleBatchJob {
  private val logger = Logger(this.getClass)
  private val baseUrl = "s3://sxl-alb-log/wmp/AWSLogs/112673975442/elasticloadbalancing/cn-north-1"

  private val logRegex =
    "(\\S+) (\\S+) (\\S+ ){10}\"(\\w+) (\\w+)://([^/\\s]+)(/[^\\?\\s]*)?(\\?(?<qs>\\S*))? .*".r

  def parseRecord(line: String) = {
    logRegex.findFirstMatchIn(line).flatMap {
      (m) =>
        val ts = m.group(2)
        val domain = m.group(6)
        if (domain == "t.sxl.cn:443") {
          val path = m.group(7)
          val values = parseQueryString(m.group(9))
          (values.get("aid"), values.get("cid"), values.get("sid")) match {
            case (Some(aid), Some(cid), Some(sid)) =>
              Some(Record(
                new DateTime(ts).getMillis,
                aid.toInt, cid, sid, 1, path, values
              ))
            case _ => None
          }
        } else None
    }
  }

  def parseRecords(content: String) = {
    content.split("\n").flatMap {
      line =>
        try {
          parseRecord(line)
        } catch {
          case e: Throwable =>
            None
        }
    }
  }

  def parseQueryString(qs: String) = {
    import java.net.URLDecoder
    qs.split("&").flatMap { v =>
      v.split("=", 2).map(s => URLDecoder.decode(s, "UTF-8")) match {
        case Array(k, v) => Some((k, v))
        case _ => None
      }
    } toMap
  }

  def shouldSplit(records: Seq[Record], record: Record) = {
    // add new session when records is empty
    (records.isEmpty ||
      // add new session when there is a long gap between two events and the next event is pv
      ((record.timestamp - records.last.timestamp) > 60000) ||
      // add new session when open id of two events are different
      ((!records.last.values.getOrElse("uoid", "").isEmpty) &&
        (!record.values.getOrElse("uoid", "").isEmpty) &&
        (records.last.values("uoid") != record.values("uoid"))))
  }

  def getSessions(records: Seq[Record]) = {
    val recSeq = records.foldLeft(Seq[Seq[Record]]()) {
      (res, rec) =>
        if (res.isEmpty) Seq(Seq(rec))
        else if (shouldSplit(res.last, rec)) res :+ Seq(rec)
        else res.updated(res.size - 1, res.last :+ rec)
    }

    recSeq.flatMap {
      (recs) =>
        recs.map(_.values.getOrElse("uoid", "")).find(!_.isEmpty).map {
          (openId) => SessionState(openId = openId, events = recs, closed = true)
        } collect {
          case (state) if state.shouldEmit => state.getSession
        }
    }
  }

  def getCSTDate(ts: Long) = (((ts / 3600 / 1000) + 8) / 24).toInt

  def addToAggregation(agg: AggregationRecord, session: Session) =
    AggregationRecord(
      sumDuration = agg.sumDuration + session.getAggregation.duration,
      sumPageviews = agg.sumPageviews + session.getAggregation.pageviews,
      sumSharings = agg.sumSharings + session.getAggregation.sharings,
      sumLikes = agg.sumLikes + session.getAggregation.likes,
      sumTotal = agg.sumTotal + session.getAggregation.total,
      countSession = agg.countSession + 1,
      maxTimestamp = math.max(agg.maxTimestamp, session.timestamp)
    )

  def mergeAggregations(a: AggregationRecord, b: AggregationRecord) =
    AggregationRecord(
      sumDuration = a.sumDuration + b.sumDuration,
      sumPageviews = a.sumPageviews + b.sumPageviews,
      sumSharings = a.sumSharings + b.sumSharings,
      sumLikes = a.sumLikes + b.sumLikes,
      sumTotal = a.sumTotal + b.sumTotal,
      countSession = a.countSession + b.countSession,
      maxTimestamp = math.max(a.maxTimestamp, b.maxTimestamp)
    )



  def getRecords(sc: SparkContext, s: DateTime, e: DateTime, namespaces: Set[String]) = {
    val start = s.withZone(DateTimeZone.UTC).withTimeAtStartOfDay
    val end = e.withZone(DateTimeZone.UTC).withTimeAtStartOfDay

    val days = for (i <- 0 to Days.daysBetween(start, end).getDays) yield start.plusDays(i)
    val dirs = days.map(d => s"${baseUrl}/${d.toString("yyyy/MM/dd")}").toSet

    logger.info(s"event=get_records start=$start end=$end dirs=$dirs")
    val files = dirs.map(path => sc.textFile(path))
    val contents = files.reduce((a, b) => a.union(b))
    contents
      .flatMap(parseRecords)
      .filter(rec => namespaces.contains(rec.namespace))
  }

  def getSessions(records: RDD[Record]): RDD[Session] = {
    records
      .groupBy(rec => (rec.appId, rec.clientId, rec.sessionId))
      .flatMap {
        case (_, iter) =>
          getSessions(iter.toSeq.sortBy(_.timestamp))
      }
  }

  def run(option: RippleJobOption, sp: StateProvider) {
    val conf = new SparkConf().setAppName(option.appName)
    val sc = new SparkContext(conf)
    val state = sp.listener.syncAndGetState.get

    val today = DateTime.now.withZone(DateTimeZone.forID("Asia/Shanghai")).withTimeAtStartOfDay
    val last =
      if (today.minusDays(5).isAfter(0)) today.minusDays(5)
      else (new DateTime(state.watermark)).withZone(DateTimeZone.forID("Asia/Shanghai")).withTimeAtStartOfDay

    if (last.isAfter(today)) {
      logger.info(s"event=no_need_to_build last=$last today=$today")
      return
    }

    val interval = new Interval(last, today)
    logger.info(s"event=build_aggregations interval=$interval namespaces=${option.namespaces}")
    val records = getRecords(sc, last.minusHours(1), today.plusHours(1), option.namespaces)
    val sessions = getSessions(records).filter(s => interval.contains(s.timestamp))

    val basicAgg = sessions
      .map(s => ((s.appId, s.openId, getCSTDate(s.timestamp)), s))
      .aggregateByKey(AggregationRecord())(addToAggregation _, mergeAggregations _)
      .map{
        case ((appId, openId, cstDate), v) => ((appId, openId), v.copy(cstDate = cstDate))
      } groupByKey

    basicAgg.foreach {
      case ((appId, openId), iter) =>
        DataWriter.writeAggregations(option.appName, appId, openId, iter.toSeq)
    }

    sp.mutator.updateWatermark(today.getMillis)
  }
}