package ink.baixin.ripple.spark

import scala.util.matching.UnanchoredRegex
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.models.{AggregationRecord, Record, Session, TopItemRecord}
import org.joda.time.{DateTime, DateTimeZone, Days, Interval}
import org.apache.spark._
import org.apache.spark.rdd.RDD

object RippleBatchJob {
  private val logger = Logger(this.getClass)
  private val baseUrl = "s3://sxl-alb-log/wmp/AWSLogs/112673975442/elasticloadbalancing/cn-north-1"
  // use `.unanchored` can match substring of input instead of entire input
  private val postMatcher = "postDetail\\?postId=(\\d+)".r.unanchored
  private val productMatcher = "productDetail\\?productId=(\\d+)".r.unanchored

  private val logRegex =
    "(\\S+) (\\S+) (\\S+ ){10}\"(\\w+) (\\w+)://([^/\\s]+)(/[^\\?\\s]*)?(\\?(?<qs>\\S*))? .*".r

  def parseRecord(line: String) = {
    // parse record with regex
    logRegex.findFirstMatchIn(line).flatMap {
      (m) =>
        val ts = m.group(2)
        val domain = m.group(6)
        if (domain == "t.sxl.cn:443") {
          val path = m.group(7)
          val values = parseQueryString(m.group(9))
          (values.get("aid"), values.get("cid"), values.get("sid")) match {
            // there must exist aid, cid and sid
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

  /**
    * whether we should split a event to different sessions
    *
    * @param records
    * @param record
    * @return
    */
  def shouldSplit(records: Seq[Record], record: Record) = {
    // add new session when records is empty
    records.isEmpty ||
      // add new session when there is a long gap between two events and the next event is pv
      ((record.timestamp - records.last.timestamp) > 60000) ||
      // add new session when open id of two events are different, this is a small probability event.
      ((!records.last.values.getOrElse("uoid", "").isEmpty) &&
        (!record.values.getOrElse("uoid", "").isEmpty) &&
        (records.last.values("uoid") != record.values("uoid")))
  }

  def getSessions(records: Seq[Record]) = {
    // get events's seq to divide into several sessions
    val recSeq = records.foldLeft(Seq[Seq[Record]]()) {
      (res, rec) =>
        if (res.isEmpty) Seq(Seq(rec))
        else if (shouldSplit(res.last, rec)) res :+ Seq(rec)
        // when time between two events is less than 60s, just update, not split into a new session
        else res.updated(res.size - 1, res.last :+ rec)
    }

    recSeq.flatMap {
      (recs) =>
        recs.map(_.values.getOrElse("uoid", "")).find(!_.isEmpty).map {
          (openId) =>
            val wrapped = Seq(SessionState.makeOpenRecord(recs.head)) ++ recs
            SessionState(openId = openId, events = wrapped, closed = true)
        } collect {
          case (state) if state.shouldEmit => state.getSession
        }
    }
  }

  def getCSTDate(ts: Long) = (((ts / 3600 / 1000) + 8) / 24).toInt

  /**
    * add session to aggregation
    * @param agg
    * @param session
    * @return
    */
  def addToAggregation(agg: AggregationRecord, session: Session) =
    agg.copy(
      sumDuration = agg.sumDuration + session.getAggregation.duration,
      sumPageviews = agg.sumPageviews + session.getAggregation.pageviews,
      sumSharings = agg.sumSharings + session.getAggregation.sharings,
      sumLikes = agg.sumLikes + session.getAggregation.likes,
      sumTotal = agg.sumTotal + session.getAggregation.total,
      countSession = agg.countSession + 1,
      maxTimestamp = math.max(agg.maxTimestamp, session.timestamp)
    )

  def getTopItems(sessions: Seq[Session], matcher: UnanchoredRegex) = {
    val records = sessions.flatMap {
      (session) =>
        session.events.collect {
          case e if e.`type` == "pv" => (e.subType, e.timestamp, e.dwellTime)
        } collect {
          case (matcher(itemId), timestamp, dwellTime) =>
            TopItemRecord(
              itemId = itemId.toInt,
              cstDate = getCSTDate(timestamp),
              dwellTime = dwellTime,
              visitTimes = 1
            )
        }
    }

    // group by itemid and cst date and reduce to aggregate
    records.groupBy(rec => (rec.itemId, rec.cstDate)).map {
      case (_, recs) =>
        recs.reduce {
          (a, b) =>
            a.copy(
              dwellTime = a.dwellTime + b.dwellTime,
              visitTimes = a.visitTimes + b.visitTimes
            )
        }
    }
  }

  /**
    * get record of last several days, because S3 are UTC timezone, so we must handle more data
    * to make it involve CST's day range.
    * @param sc
    * @param s
    * @param e
    * @param namespaces
    * @return
    */
  def getRecords(sc: SparkContext, s: DateTime, e: DateTime, namespaces: Set[String]) = {
    val start = s.withZone(DateTimeZone.UTC).withTimeAtStartOfDay
    val end = e.withZone(DateTimeZone.UTC).withTimeAtStartOfDay

    val days = for (i <- 0 to Days.daysBetween(start, end).getDays) yield start.plusDays(i)
    // all the dirs in S3 to handle
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
      // set aid, cid and sid as partition key
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
    // if watermark of state is before 5 days ago, then set 5 days ago as the last time,
    // or set watermark of state as the last time, that is we at most handle 5 days data
    val last = if (today.minusDays(5).isAfter(state.watermark)) today.minusDays(5)
    else (new DateTime(state.watermark)).withZone(DateTimeZone.forID("Asia/Shanghai")).withTimeAtStartOfDay

    if (last.isAfter(today)) {
      logger.info(s"event=no_need_to_build last=$last today=$today")
      return
    }

    val interval = new Interval(last, today)
    logger.info(s"event=build_aggregation=s interval=$interval namespaces=${option.namespaces}")
    // get two UTC days records
    val records = getRecords(sc, last.minusHours(1), today.plusHours(1), option.namespaces)
    // divide records into several sessions, then filter sessions to make them belong to CST's time range.
    val sessions = getSessions(records).filter(s => interval.contains(s.timestamp)).persist

    val basicAgg = sessions
      // group by appid and openid
      .groupBy(s => (s.appId, s.openId))
      .foreach {
        case ((appId, openId), iter) =>
          val sess = iter.toSeq
          // get session aggregation group by cst date
          val aggs = sess
            .groupBy(s => getCSTDate(s.timestamp))
            .map {
              case (dt, iter) =>
                iter.foldLeft(AggregationRecord(cstDate = dt))(addToAggregation _)
            }
          val topItems = Map(
            "posts" -> getTopItems(sess, postMatcher).toSeq,
            "products" -> getTopItems(sess, productMatcher).toSeq
          )
          // put all aggregation data into DynamoDB
          DataWriter.writeAggregations(option.appName, appId, openId, aggs.toSeq, topItems)
      }

    sp.mutator.updateWatermark(today.getMillis)
  }
}