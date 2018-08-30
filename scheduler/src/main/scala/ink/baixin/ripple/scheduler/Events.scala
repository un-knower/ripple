package ink.baixin.ripple.scheduler

import akka.actor.ActorRef

object Events {
  abstract sealed class Event(trigger: ActorRef)

  case class TaskScheduled(task: TaskMessage, trigger: ActorRef) extends Event(trigger)
}