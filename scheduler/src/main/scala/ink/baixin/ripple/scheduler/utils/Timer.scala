package ink.baixin.ripple.scheduler
package utils

import akka.actor.{ActorRef, ActorSystem}
import org.joda.time.{DateTime, DateTimeZone, Interval}
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

class Timer(system: ActorSystem, actor: ActorRef, schemes: Seq[Timer.TimerInterval]) {
  import system.dispatcher

  val cancelHandlers = schemes.map {
    case Timer.Every(delay, freq, message) => scheduleEvery(delay, freq, message)
    case Timer.Hourly(delay, message) => scheduleHourly(delay, message)
    case Timer.Daily(delay, message) => scheduleDaily(delay, message)
    case Timer.Weekly(delay, message) => scheduleWeekly(delay, message)
    case _ => throw new UnsupportedOperationException()
  }

  def scheduleEvery(delay: FiniteDuration, freq: FiniteDuration, message: AnyRef) =
    system.scheduler.schedule(delay, freq, actor, message)

  def scheduleHourly(delay: FiniteDuration, message: AnyRef) = {
    val now = DateTime.now(DateTimeZone.UTC)
    val currentHour = TimeUtil.floor(now)
    val nextTick = currentHour.plus(delay.toMillis)

    val initialDelay = if (nextTick.isAfter(now)) {
      new Interval(now, nextTick)
    } else {
      new Interval(now, nextTick.plusHours(1))
    }
    system.scheduler.schedule(initialDelay.toDurationMillis.millis, 1 hour, actor, message)
  }

  def scheduleDaily(delay: FiniteDuration, message: AnyRef) = {
    val now = DateTime.now(DateTimeZone.UTC)
    val today = now.withTimeAtStartOfDay()
    val nextTick = today.plus(delay.toMillis)

    val initialDelay = if (nextTick.isAfter(now)) {
      new Interval(now, nextTick)
    } else {
      new Interval(now, nextTick.plusDays(1))
    }
    system.scheduler.schedule(initialDelay.toDurationMillis.millis, 1 day, actor, message)
  }

  def scheduleWeekly(delay: FiniteDuration, message: AnyRef) = {
    val now = DateTime.now(DateTimeZone.UTC)
    val today = now.withTimeAtStartOfDay()
    val thisSunday = today.minusDays(today.getDayOfWeek % 7)
    val nextTick = thisSunday.plus(delay.toMillis)

    val initialDelay = if (nextTick.isAfter(now)) {
      new Interval(now, nextTick)
    } else {
      new Interval(now, nextTick.plusWeeks(1))
    }
    system.scheduler.schedule(initialDelay.toDurationMillis.millis, 7 days, actor, message)
  }
}

object Timer {
  sealed abstract class TimerInterval

  case class Every(delay: FiniteDuration, freq: FiniteDuration, message: AnyRef) extends TimerInterval
  case class Hourly(delay: FiniteDuration, message: AnyRef) extends TimerInterval
  case class Daily(delay: FiniteDuration, message: AnyRef) extends TimerInterval
  case class Weekly(delay: FiniteDuration, message: AnyRef) extends TimerInterval

  def schedule(schemes: TimerInterval*)(implicit system: ActorSystem, actor: ActorRef) =
    new Timer(system, actor, schemes)
}