package ink.baixin.ripple.scheduler.actors

import akka.actor.Actor
import com.typesafe.scalalogging.Logger

class TaskActor extends Actor {
  private val logger = Logger(this.getClass)

  override def preStart(): Unit = {
    logger.info(s"event=task_actor_start actor=${self.path}")
  }

  override def postStop(): Unit = {
    logger.info(s"event=task_actor_stop actor=${self.path}")
  }

  override def postRestart(reason: Throwable): Unit = {
    logger.info(s"event=task_actor_restart actor=${self.path} reason=${reason}")
  }

  override def receive: Receive = {
    case m =>
      logger.warn(s"event=task_actor_unrecognized_message actor=${self.path} message=$m")
  }

}
