package ink.baixin.ripple.core

import com.typesafe.scalalogging.Logger

import state._

trait StateListener {
  private val logger = Logger(this.getClass)
  protected val listenInterval: Int = 30 * 1000

  private lazy val timelySyncTask = {
    val timer = new java.util.Timer()
    val task = new java.util.TimerTask {
      override def run = {
        syncAndGetState
      }
    }
    // sync state every 30 seconds
    timer.schedule(task, 0, listenInterval)
    task
  }

  def start = {
    logger.info("event=start_listening_state")
    timelySyncTask
  }

  def getState: Option[State]
  def syncAndGetState: Option[State]
}