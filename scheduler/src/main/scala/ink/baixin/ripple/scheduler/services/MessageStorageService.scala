package ink.baixin.ripple.scheduler
package services

import awscala.dynamodbv2.{AttributeType, DynamoDB, Table}
import com.typesafe.scalalogging.Logger
import org.joda.time.DateTime
import spray.json._

object MessageStorageService extends DefaultJsonProtocol {
  private val logger = Logger(this.getClass)

  implicit val messageJsonFormat = jsonFormat4(TaskMessage.apply)

  lazy implicit val storage = DynamoDB.at(AWSConfig.region)

  lazy val table = ensureTable

  def ensureTable: Table = {
    storage.table(MessageStorageConfig.table) match {
      case Some(table) =>
        table
      case None =>
        logger.info("event=message_storage_service_create_table")
        storage.createTable(
          name = MessageStorageConfig.table,
          hashPK = "uid" -> AttributeType.String
        ).table
    }
  }

  def list: Seq[(String, Long, Option[TaskMessage])] = {
    table.scan(Seq()).map { item =>
      val status = item.attributes.find(_.name == "status") match {
        case Some(attr) => attr.value.getS
        case _ => "unknown"
      }

      val timestamp = item.attributes.find(_.name == "timestamp") match {
        case Some(attr) => attr.value.getN.toLong
        case _ => 0L
      }

      val message = item.attributes.find(_.name == "message") match {
        case Some(attr) => Some(attr.value.getS.parseJson.convertTo[TaskMessage])
        case _ => None
      }

      (status, timestamp, message)
    }
  }

  def put(status: String, message: TaskMessage) = {
    table.put(
      message.uid,
      "status" -> status,
      "timestamp" -> DateTime.now.getMillis,
      "message" -> message.toJson.compactPrint
    )
  }

  def init(msg: TaskMessage) = put("init", msg)

  def delete(msg: TaskMessage) = table.delete(msg.uid)
}
