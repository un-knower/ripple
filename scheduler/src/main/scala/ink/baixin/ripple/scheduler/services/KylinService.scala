package ink.baixin.ripple.scheduler.services

import java.net.URLEncoder

import akka.http.scaladsl.Http
import spray.json._
import akka.http.scaladsl.model._
import akka.util.ByteString
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.scheduler.KylinConfig
import org.joda.time.{Interval, DateTime => JodaDateTime}
import spray.json.{DefaultJsonProtocol, JsonFormat, JsonReader}

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Random, Success, Try}
import scala.concurrent.duration._

object KylinService {

  import KylinJsonProtocol._

  private val logger = Logger(this.getClass)
  val requestHeaders = List(headers.Authorization(headers.BasicHttpCredentials(KylinConfig.username, KylinConfig.password)))

  def getCube(name: String) = get[Cube](s"/kylin/api/cubes/${encode(name)}")

  def getJob(jobId: String) = get[Job](s"/kylin/api/jobs/${encode(jobId)}")

  def triggerCubeBuild(name: String, timeRange: Interval) =
    request[Job, CubeBuildPayload](
      method = HttpMethods.PUT,
      path = s"/kylin/api/cubes/${encode(name)}/build",
      payload = CubeBuildPayload("BUILD", timeRange)
    )

  def triggerCubeRefresh(name: String, timeRange: Interval) =
    request[Job, CubeBuildPayload](
      method = HttpMethods.PUT,
      path = s"/kylin/api/cubes/${encode(name)}/build",
      payload = CubeBuildPayload("REFRESH", timeRange)
    )

  def awaitJob(job: Job, timeout: FiniteDuration = 120 minutes) =
    Await.result(JobWatcher.startWatchingJob(job, timeout), timeout)

  def discardJob(jobId: String) =
    request[Job, String](
      method = HttpMethods.PUT,
      path = s"/kylin/api/jobs/${encode(jobId)}/cancel"
    )

  def updateCubeCache(cube: String): Try[Seq[HttpResponse]] = {
    val queryPeers = AWSService.getELBTargetsURIs(KylinConfig.targetGroupArn)
    updateCubeCache(cube, queryPeers)
  }

  def updateCubeCache(cube: String, peers: Seq[String]): Try[Seq[HttpResponse]] = {
    val tasks = peers.map { baseUrl =>
      // we won't wait for request result here, just send them and hope it will be ok
      logger.info(s"service=kylin_service event=update_cube_cache cube=$cube peer=$baseUrl")
      Http().singleRequest(
        HttpRequest(
          method = HttpMethods.PUT,
          uri = s"$baseUrl/kylin/api/cache/cube/${encode(cube)}/update",
          headers = requestHeaders
        )
      )
      Try(Await.result(Future.sequence(tasks), 2 minutes))
    }
  }

  def get[R: JsonReader](path: String, params: Map[String, String] = Map()) =
    request[R, String](HttpMethods.GET, path, params)

  def request[R: JsonReader, T: JsonFormat](
                                             method: HttpMethod,
                                             path: String,
                                             params: Map[String, String] = Map(),
                                             payload: T = ""
                                           ): Try[R] = Try {
    val entity = payload match {
      case s: String if s.isEmpty =>
        HttpEntity.Empty
      case _ =>
        HttpEntity(
          ContentTypes.`application/json`,
          payload.toJson.compactPrint.getBytes
        )
    }

    val req = HttpRequest(
      method = method,
      uri = getURI(path, params),
      entity = entity,
      headers = requestHeaders
    )
    logger.debug(s"event=kylin_service_start_request request=${req.uri}")
    val resp = Await.result(Http().singleRequest(req), 5 minutes)
    resp match {
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        val content = Await.result(entity.dataBytes.runFold(ByteString(""))(_ ++ _), 10 minutes)
        content.utf8String.parseJson.convertTo[R]
      case HttpResponse(code, _, entity, _) =>
        val content = Await.result(entity.dataBytes.runFold(ByteString(""))(_ ++ _), 10 minutes).utf8String
        logger.error(s"event=kylin_service_request_error uri=${req.uri} code=$code message=$content")
        throw new Exception(s"Kylin service API call failed with status code: $code")
    }
  }

  def getURI(path: String, params: Map[String, String]): String = {
    val paramsString = params
      .filter(param => !(param._1.isEmpty || param._2.isEmpty))
      .map(param => s"${encode(param._1)}=${encode(param._2)}")
      .mkString("&")

    if (paramsString.isEmpty) {
      s"${KylinConfig.baseUrl}$path"
    } else {
      s"${KylinConfig.baseUrl}$path?$paramsString"
    }
  }

  def encode(str: String) = URLEncoder.encode(str, "utf-8")

  object JobWatcher {
    val finishedStatus = Seq("FINISHED", "ERROR", "DISCARDED", "STOPPED")
    private val watchingJobs = mutable.HashMap[String, Future[Try[Job]]]()

    def startWatchingJob(job: Job, timeout: FiniteDuration = 90 minutes) = synchronized {
      watchingJobs.retain { case (_, v) => !v.isCompleted }
      logger.debug(s"service=kylin_service event=retain_watching_jobs remain_count=${watchingJobs.size}")
      watchingJobs.getOrElseUpdate(job.uuid, Future {
        logger.debug(s"service=kylin_service event=start_watching_job job=${job.uuid}")
        Try(updateStatus(job, System.currentTimeMillis() + timeout.toMillis))
      })
    }

    @scala.annotation.tailrec
    private def updateStatus(job: Job, timeLimit: Long, errorCount: Int = 0): Job = {
      if (System.currentTimeMillis() > timeLimit) job
      else {
        Thread.sleep(30000 + Random.nextInt(20000))
        getJob(job.uuid) match {
          case Success(newJob) =>
            if (newJob.status != job.status || newJob.progress != job.progress)
              NotificationService.kylinJobStatusUpdated(newJob)

            if (finishedStatus.contains(newJob.status)) {
              logger.info(s"service=kylin_service event=stop_watching_job job=${newJob.uuid} status=${newJob.status}")
              newJob
            } else {
              updateStatus(newJob, timeLimit)
            }
          case Failure(e) => if (errorCount <= 5) updateStatus(job, timeLimit, errorCount + 1) else throw e
        }
      }
    }

  }

}

object KylinJsonProtocol extends DefaultJsonProtocol {
  implicit val CubeJsonFormat = jsonFormat4(Cube)
}

case class Segment(uuid: String, name: String, status: String, jobId: String, dictionaries: Map[String, String],
                   storageLocation: String, timeRange: Interval)

case class Cube(uuid: String, name: String, status: String, segments: Array[Segment])

case class Job(uuid: String, name: String, typeName: String, status: String, cube: String,
               segment: String, progress: Float, timeRange: Interval, lastUpdate: JodaDateTime)

case class CubeBuildPayload(buildType: String, timeRange: Interval)