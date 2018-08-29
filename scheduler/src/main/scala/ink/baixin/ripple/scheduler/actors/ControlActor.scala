package ink.baixin.ripple.scheduler.actors

import akka.actor.Actor
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.scheduler.TaskMessage
import ink.baixin.ripple.scheduler.services.{MessageStorageService, NotificationService}

class ControlActor(target: String) extends Actor {
  private val logger = Logger(this.getClass)
  val targetActor = context.actorSelection(target)

  override def receive: Receive = {
    case ControlActor.Recover => recover
    case ControlActor.Task(msg) => pipe(msg)
  }

  /**
    * Fetch unstarted tasks from DynamoDB and run them
    */
  def recover = {
    val message = MessageStorageService.list.map {
      case ("init", timestamp, Some(msg)) => (timestamp, msg)
    } sortBy(_._1) map(_._2)

    if (message.length > 0) {
      logger.info(s"event=control_actor_recover affected_count=${message.length}")
      for (msg <- message) {
        logger.debug(s"event=control_actor_recover message=$msg")
        self ! ControlActor.Task(msg)
      }
    }
  }

  def pipe(msg: TaskMessage) = {
    logger.debug(s"event=control_actor_init_message message=$msg")
    // update message's timestamp before starting
    MessageStorageService.init(msg)
    NotificationService.taskScheduled(msg, self)
    NotificationService.taskStatusUpdated(msg, "init", self)
    logger.debug(s"event=control_actor_pass_message message=$msg")
    targetActor ! msg
  }

}

object ControlActor {
  case object Reset
  case object Recover
  case object CleanUp

  case class Task(msg: TaskMessage)
}
