package ink.baixin.ripple.scheduler
package executors

import ink.baixin.ripple.scheduler.services.{HiveService, KylinService, NotificationService}
import org.joda.time.{DateTime, Interval}
import scala.util.{Failure, Success}

object KylinCubeBuild extends TaskExecutor {
  override def execute(msg: TaskMessage): Unit = {
    val cube = msg.data("cube")
    val interval = new Interval(new DateTime(msg.data("start")), new DateTime(msg.data("end")))

    HiveService.withConnection { conn =>
      val stmt = conn.createStatement()
      val hasTimestampHour = msg.data.get("hive_no_timestamp_hour").isEmpty
      val res = if (hasTimestampHour) {
        stmt.executeQuery(
          s"""
            |SELECT 1 FROM ${cube}_parquet
            |WHERE dt >= '${interval.getStart.toString("yyyy-MM-dd")}'
            |AND dt <= '${interval.getEnd.toString("yyyy-MM-dd")}'
            |AND `timestamp_hour` >= '${interval.getStart.toString("yyyy-MM-dd HH:mm:ss")}'
            |AND `timestamp_hour` < '${interval.getEnd.toString("yyyy-MM-dd HH:mm:ss")}'
            |LIMIT 1
          """.stripMargin)
      } else {
        stmt.executeQuery(
          """
            |SELECT 1 FROM ${cube}_parquet
            |WHERE dt >= '${interval.getStart.toString("yyyy-MM-dd")}'
            |AND dt <= '${interval.getEnd.toString("yyyy-MM-dd")}'
            |LIMIT 1
          """.stripMargin)
      }

      if (!res.next()) {
        logger.warn(s"$this event=no_data_in_time_range cube=$cube interval=$interval")
        return // return directly if no data in time range
      }
    }

    val overlaps = KylinService.getCube(cube).get.segments.find(_.timeRange.overlaps(interval))
    if (overlaps.isDefined) {
      logger.warn(s"$this event=build_segment_overlaps build=$interval conflict=${overlaps.get.timeRange}")
      return
    }

    logger.info(s"$this event=trigger_cube_build cube=$cube interval=$interval")
    val job = KylinService.triggerCubeBuild(cube, interval).get
    NotificationService.kylinJobTriggered(job, task)

    logger.info(s"$this event=await_build_job cube=$cube job_id=${job.uuid}")
    KylinService.awaitJob(job) match {
      case Success(fJob) =>
        logger.info(s"$this event=build_job_terminated cube=$cube job_id=${fJob.uuid} status=${fJob.status}")
        if (fJob.status != "FINISHED") {
          // failed to finish the job successfully, discard the job immediately in case
          // it affects future maintenance
          KylinService.discardJob(fJob.uuid)
          throw new Exception(s"Kylin job ${fJob.uuid} has terminated with status: ${fJob.status}")
        }
      case Failure(e) =>
        KylinService.discardJob(job.uuid)
        throw e
    }
  }
}