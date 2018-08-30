package ink.baixin.ripple.scheduler

import akka.actor.Props
import akka.routing.FromConfig
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.scheduler.actors.ControlActor.{CleanUp, LazyTask}
import ink.baixin.ripple.scheduler.actors.{ControlActor, DispatchActor, TaskActor}
import ink.baixin.ripple.scheduler.utils.Timer
import ink.baixin.ripple.scheduler.utils.Timer.{Daily, Every, Hourly, Weekly}
import scala.concurrent.{Await, TimeoutException}
import scala.concurrent.duration._

object Application {
  import ActorContext._

  private val logger = Logger(this.getClass)

  val fastRouter = actorSystem.actorOf(FromConfig.props(Props[TaskActor]), "fast-task-router")
  val slowRouter = actorSystem.actorOf(FromConfig.props(Props[TaskActor]), "slow-task-router")
  val dispatchActor = actorSystem.actorOf(
    Props(classOf[DispatchActor], fastRouter.path.toString, slowRouter.path.toString), "dispatch-actor"
  )
  implicit val controlActor = actorSystem.actorOf(
    Props(classOf[ControlActor], dispatchActor.path.toString), "control-actor"
  )

  def main(args: Array[String]): Unit = {
    // recover from DynamoDB  before any new tasks start
    controlActor ! ControlActor.Recover
    scheduleTasks
    sys.addShutdownHook(terminate)
  }

  def scheduleTasks = {
    // Schedule wmp_data_build every day
    // This task will refresh all 90 days data
    val wmpDataRefresh = TaskConfig.wmpTask match {
      case Some(data) =>
        val rate = TaskConfig.wmpRefreshRate
        val interval = TaskConfig.wmpRefreshInterval
        for (i <- 0 until rate) yield Daily(
          (((16 + i * interval) % 24) hours) + (30 minutes),
          LazyTask("plan_wmp_data_refresh", "kylin.wmp_analytics", data)
        )
      case None => Seq()
    }

    val dataRefresh = TaskConfig.kylinCubes.map {
      case cube =>
        Hourly(15 minutes,
          LazyTask("plan_data_refresh", s"kylin.$cube",
            Map("project" -> TaskConfig.kylinProject, "cube" -> cube)
          ))
    }

    val cubeMaintenance = TaskConfig.kylinCubes.zipWithIndex.map {
      case (cube, i) =>
        Every((i + 3) hours, 1 day,
          LazyTask("plan_cube_maintenance", s"kylin.$cube",
            Map("project" -> TaskConfig.kylinProject, "cube" -> cube)))
    }

    val dimeJobs = Seq(Daily(18 hours, LazyTask("dime_data_refresh")))

    // system maintenance tasks are all blocking
    val systemMaintenance = Seq(
      Daily(5 minutes, CleanUp),
      Daily(2 hours, LazyTask("kylin_metadata_backup")),
      // after a backup is done
      Daily(3 hours, LazyTask("kylin_metadata_cleanup")),
      // schedule kylin_hbase_table_cleanup three times per week
      Weekly((1 days) + (2 hours), LazyTask("kylin_hbase_table_cleanup")),
      Weekly((3 days) + (2 hours), LazyTask("kylin_hbase_table_cleanup")),
      Weekly((6 days) + (2 hours), LazyTask("kylin_hbase_table_cleanup"))
    )

    Timer.schedule(wmpDataRefresh, dataRefresh, cubeMaintenance, dimeJobs, systemMaintenance)
  }

  def terminate = {
    try {
      logger.info("event=terminating_actor_system")
      Await.result(actorSystem.terminate(), 10 minutes)
      logger.info("event=actor_system_terminated")
    } catch {
      case e: TimeoutException =>
        logger.warn("event=actor_system_terminate_timeout")
        sys.exit(1)
    }
  }
}
