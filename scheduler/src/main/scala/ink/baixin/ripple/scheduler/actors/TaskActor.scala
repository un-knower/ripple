package ink.baixin.ripple.scheduler.actors

import akka.actor.Actor
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.scheduler.TaskMessage
import ink.baixin.ripple.scheduler.services.{GlobalLockService, MessageStorageService, NotificationService}
import ink.baixin.ripple.scheduler.{ executors => exec}

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
    case msg: TaskMessage =>
      if (!GlobalLockService.acquire(msg.blocking)) {
        logger.warn(s"event=task_actor_acquire_lock_failed message=$msg")
      }
      preExecute(msg)
      try {
        execute(msg)
        postExecute(msg, null)
      } catch {
        case e: Throwable => postExecute(msg, e)
      } finally {
        GlobalLockService.release(msg.blocking)
      }

    case m =>
      logger.warn(s"event=task_actor_unrecognized_message actor=${self.path} message=$m")
  }

  private def preExecute(msg: TaskMessage) = {
    MessageStorageService.running(msg)
    NotificationService.taskStatusUpdated(msg, "running", self)
    logger.debug(s"event=task_actor_execute_start actor=${self.path} message=$msg")
  }

  private def execute(msg: TaskMessage) = {
    exec.get(msg.name) match {
      case Some(e) => e.execute(msg, self)
      case _ =>
        logger.warn(s"event=task_actor_executor_not_found actor=${self.path} message=$msg")
    }
  }

  private def postExecute(msg: TaskMessage, err: Throwable) = {
    if (err == null) {
      MessageStorageService.finish(msg)
      NotificationService.taskStatusUpdated(msg, "finish", self)
      logger.debug(s"event=task_actor_execute_finish actor=${self.path} message=$msg")
    } else {
      MessageStorageService.error(msg)
      NotificationService.taskStatusUpdated(msg, "error", self)
      NotificationService.taskExecutionFailed(msg, Right(err), self)
      logger.error(s"event=task_actor_execute_failure actor=${self.path} message=$msg reason=$err")
      logger.error(err.getStackTrace.toString)
    }
  }
}