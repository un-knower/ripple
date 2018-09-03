package ink.baixin.ripple.scheduler
package actors

import akka.actor.Actor
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.scheduler.Events.Event
import ink.baixin.ripple.scheduler.services.NotificationService
import ink.baixin.ripple.scheduler.utils.Debounce
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.concurrent.duration._

class DispatchActor(fastTarget: String, slowTarget: String) extends Actor {
  import context.dispatcher
  private val logger = Logger(this.getClass)

  val fastTargetActor = context.actorSelection(fastTarget)
  val slowTargetActor = context.actorSelection(slowTarget)

  // all task pipe to this queue
  val taskQueue = mutable.LinkedHashSet[TaskMessage]()
  // set priority for the key
  val keyPriority = mutable.HashMap[String, Int]()
  // assigned key and current time
  val keyAssigned = mutable.HashMap[String, Long]()
  // store the average duration of task
  val metrics = mutable.HashMap[(String, String), Long]()
  val recentDurations = ListBuffer[Long]()
  var slowValue = (10 minutes).toMillis

  val random = Random

  override def preStart() = {
    logger.info("actor=dispatch event=start_monitoring_tasks")
    NotificationService.subscribe(self, classOf[Event])
  }

  override def receive: Receive = {
    case e: Events.Event =>
      handleEvent(e)
      delayedAssignTasks
    case msg: TaskMessage =>
      addTask(msg)
      delayedAssignTasks
    case _ =>
      logger.debug("actor=dispatch event=received_general_message")
      // try assign tasks every time a new message is received
      assignTasks

  }

  def handleEvent(e: Events.Event) = e match {
    case Events.TaskStatusUpdated(task, "finish", _) =>
      // update metrics for key and name
      if (!task.hashKey.isEmpty)
        updateMetrics(task.hashKey, task.name)
      // captured task finished event, remove hashkey from assigned keys
      keyAssigned.remove(task.hashKey)
    case Events.TaskExecutionFailed(task, _, _) if keyAssigned.contains(task.hashKey) =>
      // captured task error event, it is likely that the corresponding key might have
      // some serious problems, we clear its priority.
      keyPriority.put(task.hashKey, 0)
      keyAssigned.remove(task.hashKey)
    case _ => // pass
  }

  def updateMetrics(key: String, name: String) = {
    val record = keyAssigned.get(key)
    if (!record.isEmpty) {
      // to millsecond
      val duration = (System.nanoTime() - record.get) / 1e6.toLong
      // get the average time of task
      metrics.put(
        (key, name),
        (metrics.getOrElse((key, name), duration) + duration) / 2
      )
      logger.info(s"actor=dispatch event=update_metrics key=$key name=$name metrics=${metrics((key, name))}")
      recentDurations.append(duration)
      // set recentDurations not larger than 1000
      recentDurations.trimStart(math.max(0, recentDurations.size - 1000))
    }

    if (recentDurations.length > 10) {
      val durationsCopy = recentDurations.clone().sorted
      var sum = durationsCopy.sum / 2
      while (sum >= 0) {
        sum -= durationsCopy.remove(0)
      }
      if (!durationsCopy.isEmpty) {
        slowValue = durationsCopy.head
      }
      logger.info(s"actor=dispatch event=update_value recent=${recentDurations.length} value=$slowValue")
    }
  }

  def addTask(msg: TaskMessage) = {
    taskQueue.add(msg)
    // assign an initial priority for a new key
    keyPriority.put(
      msg.hashKey,
      keyPriority.getOrElse(msg.hashKey, 0) + 5 + random.nextInt(5)
    )
    logger.info(s"actor=dispatch event=add_task priority=${keyPriority(msg.hashKey)} queue_size=${taskQueue.size} msg=$msg")
  }

  def delayedAssignTasks = {
    context.system.scheduler.scheduleOnce(6 seconds, self, "")
  }

  def assignTasks = Debounce.debounce(5 seconds) {
    logger.debug("actor=dispatch event=assign_tasks")
    // retain assigned keys in 5 hours
    val retainLimit = System.nanoTime() - (300 minutes).toNanos
    keyAssigned.retain {
      (_, time) => time > retainLimit
    }

    if (keyAssigned.contains("") || taskQueue.isEmpty) {
      // blocking task is running or no pending tasks, wait for some time
      context.system.scheduler.scheduleOnce(60 seconds, self, "")
    } else if (taskQueue.head.hashKey.isEmpty) {
      // empty is a special key, send it if presented at the head
      val msg = taskQueue.head
      logger.info(s"actor=dispatch event=forward_blocking_task task=$msg")
      slowTargetActor ! msg
      taskQueue.remove(msg)
      keyAssigned.put("", System.nanoTime())
    } else {
      keyPriority.toSeq.sortBy()
    }

  }

}