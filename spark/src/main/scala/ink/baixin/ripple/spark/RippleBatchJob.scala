package ink.baixin.ripple.spark

import com.typesafe.scalalogging.Logger
import org.joda.time.{ DateTime, Days, Interval, DateTimeZone }
import org.apache.spark._
import org.apache.spark.rdd.RDD
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.models.{ Record, AggregationRecord }

case class AggSession(
                       appId: Int,
                       openId: String,
                       pageviews: Int,
                       sharings: Int,
                       likes: Int,
                       total: Int,
                       starttime: Long,
                       endtime: Long
                     ) {
  val duration = (endtime - starttime).toInt
}


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
                aid.toInt, cid, sid, 0, path, values
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

  def shouldSplit(records: Seq[AggSession], record: Record) = {
    // add new session when records is empty
    (records.isEmpty ||
      // add new session when there is a long gap between two events and the next event is pv
      ((record.timestamp - records.last.endtime) > 60000) ||
      // add new session when open id of two events are different
      ((!records.last.openId.isEmpty) &&
        (!record.values.getOrElse("uoid", "").isEmpty) &&
        (records.last.openId != record.values.getOrElse("uoid", ""))))
  }

  def getSessions(records: Seq[Record]) = {
    records.foldLeft(Seq[AggSession]()) {
      (res, rec) =>
        val (nres, sess) =
          if (shouldSplit(res, rec)) {
            val s = AggSession(
              rec.appId,
              rec.values.getOrElse("uoid", ""),
              0, 0, 0, 0, rec.timestamp, rec.timestamp
            )
            (res, s)
          } else {
            val openId = if (res.last.openId.isEmpty) {
              rec.values.getOrElse("uoid", "")
            } else {
              res.last.openId
            }
            (res.slice(0, res.length - 1), res.last.copy(openId = openId))
          }

        val typ = rec.values.getOrElse("et", "")
        val subtype = rec.values.getOrElse("est", "")

        if (typ == "pv") {
          nres :+ sess.copy(
            pageviews = sess.pageviews + 1,
            total = sess.total + 1,
            endtime = rec.timestamp
          )
        } else if (typ == "ui") {
          nres :+ sess.copy(
            likes = sess.likes + (if (subtype.startsWith("like")) 0 else 1),
            sharings = sess.sharings + (if (subtype.startsWith("share")) 0 else 1),
            total = sess.total + 1,
            endtime = rec.timestamp
          )
        } else {
          nres :+ sess.copy(endtime = rec.timestamp)
        }
    }
  }

  def addAggSession(rec: AggregationRecord, s: AggSession) = {
    rec.copy(
      sumDuration = rec.sumDuration + s.duration,
      sumPageviews = rec.sumPageviews + s.pageviews,
      sumSharings = rec.sumSharings + s.sharings,
      sumLikes = rec.sumLikes + s.likes,
      sumTotal = rec.sumTotal + s.total,
      countSession = rec.countSession + 1,
      maxStarttime = math.max(rec.maxStarttime, s.starttime)
    )
  }

  def mergeAggRecord(a: AggregationRecord, b: AggregationRecord) = {
    AggregationRecord(
      sumDuration = a.sumDuration + b.sumDuration,
      sumPageviews = a.sumPageviews + b.sumPageviews,
      sumSharings = a.sumSharings + b.sumSharings,
      sumLikes = a.sumLikes + b.sumLikes,
      sumTotal = a.sumTotal + b.sumTotal,
      countSession = a.countSession + b.countSession,
      maxStarttime = math.max(a.maxStarttime, b.maxStarttime)
    )
  }

  def buildTimeRange(sc: SparkContext, tr: Interval, appName: String, namespaces: Set[String]) {
    val records = getRecords(
      sc,
      tr.getStart.minusHours(1).withZone(DateTimeZone.UTC).withTimeAtStartOfDay,
      tr.getEnd.plusHours(1).withZone(DateTimeZone.UTC).withTimeAtStartOfDay,
      namespaces
    )
    val sessions = getSessions(records).filter(s => (!s.openId.isEmpty) && (tr.contains(s.starttime)))

    val baseAggs = sessions
      .map(s => ((s.appId, (s.starttime / 3600000).toInt, s.openId), s))
      .aggregateByKey(AggregationRecord())(addAggSession, mergeAggRecord)

    // write app hour aggregations
    baseAggs.map {
      case (k, agg) => ((k._1, k._2), agg.copy(k._1, k._2, k._3))
    }.groupByKey.foreach {
      case ((appId, hour), iter) =>
        DataWriter.writeAggregations(appName, appId, hour, iter.toSeq)
    }

    // write app openId aggregations
    baseAggs.map {
      case (k, agg) => ((k._1, k._3, k._2 / 24), agg.copy(k._1, k._2))
    }.groupByKey.foreach {
      case ((appId, openId, date), iter) =>
        DataWriter.writeAggregations(appName, appId, openId, date, iter.toSeq)
    }
  }

  def getRecords(sc: SparkContext, start: DateTime, end: DateTime, namespaces: Set[String]) = {
    val days = for (i <- 0 to Days.daysBetween(start, end).getDays) yield start.plusDays(i)
    val dirs = days.map(d => s"${baseUrl}/${d.toString("yyyy/MM/dd")}")

    logger.info(s"event=get_records start=$start end=$end dirs=$dirs")
    val files = dirs.map(path => sc.textFile(path))
    val contents = files.reduce((a, b) => a.union(b))
    contents
      .flatMap(parseRecords)
      .filter(rec => namespaces.contains(rec.namespace))
  }

  def getSessions(records: RDD[Record]): RDD[AggSession] = {
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

    while (true) {
      logger.info(s"event=try_to_build_aggregation namespaces=${option.namespaces}")
      val state = sp.listener.syncAndGetState
      state.foreach {
        (state) =>
          val last = {
            new DateTime(math.max(state.watermark, DateTime.now.minusDays(100).getMillis))
              .withTimeAtStartOfDay
          }

          val limit = {
            new DateTime(math.min(DateTime.now.minusDays(1).getMillis, last.plusDays(10).getMillis))
              .withTimeAtStartOfDay
          }

          if (limit.isAfter(last) && Days.daysBetween(last, limit).getDays >= 1) {
            try {
              buildTimeRange(sc, new Interval(last, limit), option.appName, option.namespaces)
              logger.info(s"event=update_watermark datetime=$limit millis=${limit.getMillis}")
              sp.mutator.updateWatermark(limit.getMillis)
            } catch {
              case e: Throwable =>
                logger.error(s"event=failed_to_build_aggregation error=$e")
                // sleep for a long time before try again
                Thread.sleep(60 * 60 * 1000)
            }
          } else {
            val l = new DateTime(math.max(last.getMillis, limit.getMillis)).withTimeAtStartOfDay
            val d = l.plusDays(1)
            val n = DateTime.now
            if (d.isAfter(n)) Thread.sleep(n.getMillis - d.getMillis)
          }
      }

      if (state.isEmpty) {
        // Get state failed, sleep for a short time
        logger.error(s"event=get_state_failed")
        Thread.sleep(5 * 60 * 1000)
      }
    }
  }
}