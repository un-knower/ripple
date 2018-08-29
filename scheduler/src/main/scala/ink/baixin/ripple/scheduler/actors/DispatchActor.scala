package ink.baixin.ripple.scheduler.actors

import akka.actor.Actor

class DispatchActor(fastTarget: String, slowTarget: String) extends Actor {
  val fastTargetActor = context.actorSelection(fastTarget)
  val slowTargetActor = context.actorSelection(slowTarget)



}
