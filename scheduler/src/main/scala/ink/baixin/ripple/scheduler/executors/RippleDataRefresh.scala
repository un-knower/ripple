package ink.baixin.ripple.scheduler.executors

import ink.baixin.ripple.scheduler.TaskMessage
import ink.baixin.ripple.scheduler.services.SparkService
import org.apache.hadoop.yarn.api.records.YarnApplicationState

object RippleDataRefresh extends TaskExecutor {
  override def execute(msg: TaskMessage) = {
    val appJar = "hdfs:///apps/spark/ripple-jobs.jar"
    val argm = Map(
      "appname" -> msg.data.getOrElse("appname", "production"),
      "taskcategory" -> "batch"
    )

    SparkService.runSparkApp(appJar, "ink.baixin.ripple.spark.RippleJob", argm, 2) match {
      case Some(state) if (state == YarnApplicationState.FINISHED) =>
        logger.info(s"$this event=ripple_job_finished status=$state")
      case Some(state) =>
        logger.error(s"$this event=ripple_job_finished status=$state")
        throw new Exception(s"Spark App $name finished with status $state!")
      case None =>
        logger.error(s"$this event=ripple_job_failed_to_launch")
        throw new Exception(s"Spark App $name failed to launch!")
    }
  }
}
