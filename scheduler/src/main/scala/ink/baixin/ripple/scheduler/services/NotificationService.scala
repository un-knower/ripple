package ink.baixin.ripple.scheduler
package services

import akka.actor.ActorRef

object NotificationService {
  import Events._
  import ActorContext._

  def publish(event: Event) =
    actorSystem.eventStream.publish(event)

  def taskScheduled(task: TaskMessage, trigger: ActorRef = ActorRef.noSender) =
    publish(TaskScheduled(task, trigger))

  def taskStatusUpdated(task: TaskMessage, status: String, trigger: ActorRef = ActorRef.noSender) =
    publish(TaskStatusUpdated(task, status, trigger))

}
