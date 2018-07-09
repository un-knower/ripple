package ink.baixin.ripple.core

import com.typesafe.scalalogging.Logger
import state._

trait StateListener {
  private val logger = Logger(this.getClass)

  def start(interval: Int = 30 * 1000)(f: Option[State] => Unit) = {
    logger.info("event=start_listening_state")
    val timer = new java.util.Timer()
    val task = new java.util.TimerTask {
      override def run = {
        f(syncAndGetState)
      }
    }
    // sync state every interval, delayed an interval's period for the first triggering
    timer.schedule(task, 0, interval)
    task
  }

  def getState: Option[State]
  def syncAndGetState: Option[State]
}