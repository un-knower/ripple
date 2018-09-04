package ink.baixin.ripple.scheduler
package actors

import akka.actor.Actor
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.scheduler.services.{MessageStorageService, NotificationService}
import org.joda.time.{DateTime, DateTimeZone}

class ControlActor(target: String) extends Actor {
  private val logger = Logger(this.getClass)
  val targetActor = context.actorSelection(target)

  override def receive: Receive = {
    case ControlActor.Recover => recover
    case ControlActor.CleanUp => cleanup
    case ControlActor.Task(msg) => pipe(msg)
    case ControlActor.LazyTask(name, key, data) =>
      pipe(TaskMessage.create(name, key, data ++ Map("now" -> DateTime.now.toString)))
    case m =>
      logger.warn(s"event=control_actor_unsupported_message message=$m")
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

  /**
    * Remove finish, abort and error task older than 90 days
    */
  def cleanup = {
    val limit = DateTime.now(DateTimeZone.UTC).minusDays(90).getMillis
    val messages = MessageStorageService.list
      .filterNot(m => m._1 == "init" || m._1 == "running")
      .collect {
        case (_, timestamp, Some(msg)) if (timestamp < limit) => msg
      }

    if (messages.length > 0) {
      logger.info(s"event=control_actor_cleanup affected_count=${messages.length}")
      for (msg <- messages) {
        logger.debug(s"event=control_actor_delete_task message=$msg")
        MessageStorageService.delete(msg)
      }
    }
  }

  def pipe(msg: TaskMessage) = {
    logger.debug(s"event=control_actor_init_message message=$msg")
    // update message's timestamp before starting
    MessageStorageService.init(msg)
    NotificationService.taskStatusUpdated(msg, "init", self)
    logger.debug(s"event=control_actor_pass_message message=$msg")
    targetActor ! msg
  }
}

object ControlActor {
  case object Recover
  case object CleanUp

  case class Task(msg: TaskMessage)
  case class LazyTask(name: String, key: String = "", data: Map[String, String] = Map())
}
