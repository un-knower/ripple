package ink.baixin.ripple.scheduler

import akka.actor.ActorRef

object Events {
  abstract sealed class Event(trigger: ActorRef)

  case class TaskStatusUpdated(task: TaskMessage, status: String, trigger: ActorRef) extends Event(trigger)
  case class TaskExecutionFailed(task: TaskMessage, error: Either[String, Throwable], trigger: ActorRef) extends Event(trigger)
  case class KylinJobTriggered(job: services.Job, task: TaskMessage, trigger: ActorRef) extends Event(trigger)
  case class KylinJobStatusUpdated(job: services.Job, trigger: ActorRef) extends Event(trigger)
}