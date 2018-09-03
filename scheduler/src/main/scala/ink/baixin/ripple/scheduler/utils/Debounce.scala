package ink.baixin.ripple.scheduler
package utils

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.FiniteDuration

object Debounce {
  def debounce(wait: FiniteDuration)(f: => Unit) = {
    var (isRunning, lastStopTime) = (new AtomicBoolean(false), Long.MinValue)
    val doneWaiting = lastStopTime + wait.toNanos <= System.nanoTime()
    // isRunning is false && doneWaiting is true
    if (isRunning.compareAndSet(false, doneWaiting) && doneWaiting) {
      try {
        f
      } finally {
        lastStopTime = System.nanoTime()
        isRunning.set(false)
      }
    }
  }
}
