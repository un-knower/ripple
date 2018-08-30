package ink.baixin.ripple.scheduler
package utils

import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.duration._

object TimeUtil {
  def floor(dt: DateTime, unit: FiniteDuration = 1 hour) = {
    val epoch = dt.getMillis
    val interval = unit.toMillis
    new DateTime((epoch / interval) * interval).withZone(DateTimeZone.UTC)
  }

  def ceiling(dt: DateTime, unit: FiniteDuration = 1 hour) = {
    val epoch = dt.getMillis
    val interval = unit.toMillis
    val newEpoch = if (epoch % interval == 0) epoch else (epoch / interval + 1) * interval
    new DateTime(newEpoch).withZone(DateTimeZone.UTC)
  }

}
