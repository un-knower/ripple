package ink.baixin.ripple.scheduler.executors

import akka.actor.ActorRef
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.scheduler.TaskMessage

trait TaskExecutor {
  protected val logger = Logger(this.getClass)

  val name = {
    val reg = "([a-z])([A-Z]+)".r
    reg.replaceAllIn(this.getClass.getSimpleName, "$1_$2").toLowerCase().stripSuffix("$")
  }

  def execute(msg: TaskMessage, actorRef: ActorRef) = {
    logger.info(s"$this actor=${actorRef.path} event=start message=$msg")
    execute(msg)
    logger.info(s"$this actor=${actorRef.path} event=finish message=$msg")
  }

  def execute(msg: TaskMessage) = throw new UnsupportedOperationException()
}
