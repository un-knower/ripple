package ink.baixin.ripple.scheduler.executors
import akka.actor.ActorRef
import ink.baixin.ripple.scheduler.TaskMessage

object Print extends TaskExecutor {
  override def execute(msg: TaskMessage, actorRef: ActorRef) = {
    logger.info(s"$this event=execute message=$msg actor=${actorRef.path}")
  }
}
