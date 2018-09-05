package ink.baixin.ripple.scheduler
package executors

import ink.baixin.ripple.scheduler.services.KylinService
import ink.baixin.ripple.scheduler.utils.TimeUtil
import org.joda.time.{DateTime, DateTimeZone, Interval}

object PlanDataRefresh extends TaskExecutor {
  override def execute(msg: TaskMessage): Unit = {
    val now = DateTime.parse(msg.data.getOrElse("now", DateTime.now(DateTimeZone.UTC).toString()))
    // if task has been delayed for 1 hour, abort it
    if (DateTime.now(DateTimeZone.UTC).minusHours(1).isAfter(now)) {
      logger.warn(s"$this event=abort_due_to_expire message=$msg")
      return
    }

    val start = TimeUtil.floor(now.minusHours(1))
    val end = TimeUtil.floor(now)

    // submit a hive refresh task to refresh data in last one hour
    Application.submit(TaskMessage.create(
      "hive_table_refresh", msg.hashKey,
      Map("cube" -> msg.data("cube"), "start" -> start.toString(), "end" -> end.toString())
    ))

    val interval = new Interval(start, end)
    val cube = KylinService.getCube(msg.data("cube")).get
    val segments = cube.segments.sortWith {
      (a, b) => a.timeRange.getEnd.isBefore(b.timeRange.getEnd)
    }
    val coverredSegments = segments.filter(_.timeRange.overlaps(interval))

    if (coverredSegments.isEmpty) {
      // no coverred intervals, there might be holes in the segment. Try to fill it.
      val newStart = if (segments.isEmpty) start else segments.last.timeRange.getEnd
      Application.submit(TaskMessage.create(
          "kylin_cube_build", msg.hashKey,
          msg.data ++ Map("start" -> newStart.toString, "end" -> start.plusHours(4).toString)
        )
      )
    } else {
      val lastSeg = coverredSegments.last

      // last segment can not cover current interval completely, we need a new segment
      if (lastSeg.timeRange.getEnd.isBefore(end)) {
        val newStart = lastSeg.timeRange.getEnd
        Application.submit(TaskMessage.create(
          "kylin_cube_build", msg.hashKey,
          msg.data ++ Map("start" -> newStart.toString, "end" -> newStart.plusHours(4).toString)
        ))
      }

      // for every overlaped segments, we need to refresh them
      for (segment <- coverredSegments) {
        val timeRange = segment.timeRange
        Application.submit(TaskMessage.create(
          "kylin_cube_refresh", msg.hashKey,
          msg.data ++ Map("start" -> timeRange.getStart.toString, "end" -> timeRange.getEnd.toString)
        ))
      }
    }

    Application.submit(
      TaskMessage.create("kylin_cube_cache_update", msg.hashKey, Map("cube" -> msg.data("cube")))
    )
  }
}