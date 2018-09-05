package ink.baixin.ripple.scheduler
package executors

import ink.baixin.ripple.scheduler.services.KylinService
import scala.util.Success

object KylinCubeCacheUpdate extends TaskExecutor {
  override def execute(msg: TaskMessage): Unit = {
    val cube = msg.data("cube")

    val minSeg = msg.data.getOrElse("min_segments", "0").toInt
    KylinService.getCube(cube) match {
      case Success(cube) if cube.segments.length >= minSeg =>
        logger.info(s"$this event=kylin_cube_update_cache cube=$cube")
        KylinService.updateCubeCache(cube)
      case _ =>
        logger.warn(s"$this event=abort_update cube=$cube")
    }
  }
}