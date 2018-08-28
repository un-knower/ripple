package ink.baixin.ripple.scheduler

import akka.actor.ActorSystem

object ActorContext {
  implicit val actorSystem = ActorSystem("ripple-scheduler")

}
