package ink.baixin.ripple.scheduler
package services

import akka.actor.ActorRef

object NotificationService {
  import Events._
  import ActorContext._

  def subscribe(actor: ActorRef, channel: Class[_]) =
    actorSystem.eventStream.subscribe(actor, channel)

  def publish(event: Event) =
    actorSystem.eventStream.publish(event)

  def unsubscribe(actor: ActorRef) =
    actorSystem.eventStream.unsubscribe(actor)

  def unsubscribe(actor: ActorRef, channel: Class[_]) =
    actorSystem.eventStream.unsubscribe(actor, channel)

  def taskScheduled(task: TaskMessage, trigger: ActorRef = ActorRef.noSender) =
    publish(TaskScheduled(task, trigger))

  def taskStatusUpdated(task: TaskMessage, status: String, trigger: ActorRef = ActorRef.noSender) =
    publish(TaskStatusUpdated(task, status, trigger))

  def taskExecutionFailed(task: TaskMessage, error: Either[String, Throwable], trigger: ActorRef = ActorRef.noSender) =
    publish(TaskExecutionFailed(task, error, trigger))
}