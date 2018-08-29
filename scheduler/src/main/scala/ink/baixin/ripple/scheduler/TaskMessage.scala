package ink.baixin.ripple.scheduler

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
