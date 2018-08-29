package ink.baixin.ripple.scheduler

import akka.actor.Props
import akka.routing.FromConfig
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.scheduler.actors.{ControlActor, DispatchActor, TaskActor}

object Application {

  import ActorContext._

  private val logger = Logger(this.getClass)

  val fastRouter = actorSystem.actorOf(FromConfig.props(Props[TaskActor]), "fast-task-router")
  val slowRouter = actorSystem.actorOf(FromConfig.props(Props[TaskActor]), "slow-task-router")
  val dispatchActor = actorSystem.actorOf(
    Props(classOf[DispatchActor], fastRouter.path.toString, slowRouter.path.toString), "dispatch-actor"
  )
  val controlActor = actorSystem.actorOf(
    Props(classOf[ControlActor], dispatchActor.path.toString), "control-actor"
  )

  def main(args: Array[String]): Unit = {
    controlActor ! ControlActor.Recover // recover from DynamoDB  before any new tasks start

  }

}
