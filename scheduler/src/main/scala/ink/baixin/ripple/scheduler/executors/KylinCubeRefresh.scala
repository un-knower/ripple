package ink.baixin.ripple.scheduler
package executors

import ink.baixin.ripple.scheduler.services.{KylinService, NotificationService}
import org.joda.time.{DateTime, Interval}
import scala.util.{Failure, Success}

object KylinCubeRefresh extends TaskExecutor {
  override def execute(msg: TaskMessage): Unit = {
    val cube = msg.data("cube")
    val interval = new Interval(new DateTime(msg.data("start")), new DateTime(msg.data("end")))
    val segments = KylinService.getCube(cube).get.segments
    val seg = segments.find(_.timeRange.isEqual(interval)).get

    logger.info(s"$this event=trigger_cube_refresh cube=$cube interval=$interval")
    val job = KylinService.triggerCubeRefresh(cube, seg.timeRange).get
    NotificationService.kylinJobTriggered(job, msg)

    logger.info(s"$this event=await_refresh_job cube=$cube job_id=${job.uuid}")
    KylinService.awaitJob(job) match {
      case Success(fJob) =>
        logger.info(s"$this event=refresh_job_terminated cube=$cube job_id=${fJob.uuid} status=${fJob.status}")
        if (fJob.status != "FINISHED") {
          KylinService.discardJob(fJob.uuid)
          throw new Exception(s"Kylin job ${fJob.uuid} has terminated with status: ${fJob.status}")
        }
      case Failure(e) =>
        KylinService.discardJob(job.uuid)
        throw e
    }
  }
}