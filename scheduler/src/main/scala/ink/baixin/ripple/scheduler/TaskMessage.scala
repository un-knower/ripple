package ink.baixin.ripple.scheduler

import java.util.UUID

import akka.routing.ConsistentHashingRouter.ConsistentHashable

case class TaskMessage(
                        uid: String,
                        name: String,
                        hashKey: String,
                        data: Map[String, String]
                      ) extends ConsistentHashable {
  def blocking = hashKey == null || hashKey.isEmpty

  override def consistentHashKey = if (hashKey == null) "" else hashKey
}

object TaskMessage {
  def create(name: String, hashKey: String, data: Map[String, String]) =
    new TaskMessage(UUID.randomUUID().toString, name, hashKey, data)
}
