package ink.baixin.ripple.scheduler
package actors

import akka.actor.Actor
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.scheduler.Events.Event
import ink.baixin.ripple.scheduler.services.NotificationService
import scala.collection.mutable
import scala.util.Random
import scala.concurrent.duration._
import utils.Debounce.debounce

class DispatchActor(fastTarget: String, slowTarget: String) extends Actor {
  import context.dispatcher
  private val logger = Logger(this.getClass)

  val fastTargetActor = context.actorSelection(fastTarget)
  val slowTargetActor = context.actorSelection(slowTarget)

  val taskQueue = mutable.LinkedHashSet[TaskMessage]()
  val keyPriority = mutable.HashMap[String, Int]()

  val random = Random

  override def preStart() = {
    logger.info("actor=dispatch event=start_monitoring_tasks")
    NotificationService.subscribe(self, classOf[Event])
  }

  override def receive: Receive = {
    case msg: TaskMessage =>
      addTask(msg)
      delayedAssignTasks

    case _ =>
      logger.debug("actor=dispatch event=received_general_message")
      assignTasks

  }

  def addTask(msg: TaskMessage) = {
    taskQueue.add(msg)
    // assign an initial priority for a new key
    keyPriority.put(
      msg.hashKey,
      keyPriority.getOrElse(msg.hashKey, 0) + 5 + random.nextInt(5)
    )
    logger.info(s"actor=dispatch event=add_task priority=${keyPriority(msg.hashKey)} queue_size=${taskQueue.size} msg=$msg")
  }

  def delayedAssignTasks = {
    context.system.scheduler.scheduleOnce(6 seconds, self, "")
  }

  def assignTasks = debounce(5 seconds) {
    logger.debug("actor=dispatch event=assign_tasks")
  }

}
