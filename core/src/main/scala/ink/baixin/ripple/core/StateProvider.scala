package ink.baixin.ripple.core

import java.util.concurrent.atomic.AtomicReference

import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, Table}
import com.amazonaws.services.dynamodbv2.document.spec.{GetItemSpec, PutItemSpec}
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.joda.time.{DateTime, DateTimeZone}
import state._

import scala.util.{Failure, Random, Success, Try}

class StateProvider(project: String, config: Config) {
  private val logger = Logger(this.getClass.getName)
  private val stateRef = new AtomicReference[(Long, Option[State])](0, None)
  private val randomGen = new Random()

  private lazy val dynamodbClient = AmazonDynamoDBClientBuilder
    .standard()
    .withRegion(Regions.fromName(config.getString("aws-region")))
    .build()

  private lazy val metadataTable = {
    val tableName = config.getString("metadata-table")
    val tableSpec = new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(new KeySchemaElement("project", KeyType.HASH))
      .withAttributeDefinitions(
        // number type
        new AttributeDefinition("id", ScalarAttributeType.N),
        // byte type
        new AttributeDefinition("state", ScalarAttributeType.B)
      )

    if (TableUtils.createTableIfNotExists(dynamodbClient, tableSpec)) {
      logger.info(s"event=create_metadata_table name=$tableName")
    }
    // wait for the table to be ready, at most 300 seconds and check once per 5 seconds
    logger.info(s"event=wait_metadata_active name=$tableName")
    TableUtils.waitUntilActive(dynamodbClient, tableName, 300 * 1000, 5 * 1000)

    new DynamoDB(dynamodbClient).getTable(tableName)
  }

  private def syncState = stateRef.updateAndGet { old =>
    logger.info(s"event=fetch_state project=$project")
    Try(
      metadataTable.getItem(new GetItemSpec()
        .withPrimaryKey("project", project)
        .withConsistentRead(true))
    ) match {
      case Success(item: Item) =>
        val id = item.getLong("id")
        val binary = item.getBinary("state")
        logger.info(s"event=fetched_state project=$project id=$id")
        val newState = Some(State.parseFrom(binary))
        if ((id, newState) == old) {
          // state not changed, return the old tuple
          old
        } else {
          (id, newState)
        }
      case Failure(e) =>
        logger.error(s"event=fetch_state_failure error=$e")
        Thread.sleep(randomGen.nextInt(2000) + 2000) // randomly sleep for a while
        // if sync failed, just return old state
        old
    }
  }

  private def updateState(uf: Option[State] => Option[State]) = stateRef.updateAndGet { old =>
    val (oldId, oldState) = old
    val (newId, newState) = (System.currentTimeMillis, uf(oldState))

    if (newState.isEmpty || newState == oldState) {
      old // do nothing if newState is empty or not changed
    } else {
      val item = new Item()
        .withPrimaryKey("project", project)
        .withNumber("id", newId)
        .withBinary("state", newState.get.toByteArray)

      // set filter condition
      val spec = if (oldState.isEmpty) {
        new PutItemSpec()
          .withItem(item)
          .withConditionExpression("attribute_not_exists(:project)")
          .withValueMap(new ValueMap().withString(":project", project))
      } else {
        new PutItemSpec()
          .withItem(item)
          // take inaccurate time clock into consideration, forbid two id to be clear
          // here we enfore a 3 seconds gap between adjacent update id
          .withConditionExpression("attribute_exists(:project) AND id = :oldId and id < :limit")
          .withValueMap(new ValueMap()
            .withNumber(":oldId", oldId)
            .withNumber(":limit", newId - 3000)
            .withString(":project", project))
      }

      Try(metadataTable.putItem(spec)) match {
        case Success(_) =>
          // update succeed, return new state
          (newId, newState)
        case Failure(e) =>
          logger.error(s"event=failed_to_update_state project=$project state=$newState error=$e")
          syncState // fetch and sync latest state
      }
    }
  }

  private def getNewState = {
    val tz = config.getString("time-zone")
    val s = DateTime.now(DateTimeZone.forID(tz)).withTimeAtStartOfDay
    val e = s.plusDays(3)
    val seg = State.Segment(1, false, false, s.getMillis, e.getMillis)
    State(
      project = project,
      factTable = config.getString("fact-table"),
      resourceBucket = config.getString("resource-bucket"),
      reservedId = 5, // TODO why set default reservedId to 5
      segments = Seq(seg)
    )
  }

  private def getDynamodbTable(name: String) = Try {
    TableUtils.waitUntilActive(dynamodbClient, name)
    new DynamoDB(dynamodbClient).getTable(name)
  }

  def ensureState = {
    updateState { old =>
      old match {
        case None => Some(getNewState)
        case _ => old
      }
    }
  }

  lazy val listener = new StateListener {
    override def getState: Option[State] = stateRef.get._2
    override def syncAndGetState: Option[State] = syncState._2
  }

  lazy val mutator = new StateMutator {
    override def updateDelegate(uf: State => State): Option[State] = updateState {
      // only let mutator mutate an existing state
      case Some(old) =>Some(uf(old))
      case _ =>
        logger.warn(s"event=mutate_empty_state project=$project")
        None
    }._2
  }

  lazy val resolver = new ResourceResolver {
    override protected def syncAndGetStateDelegate: Option[State] = stateRef.get._2

    override protected def getStateDelegate: Option[State] = syncState._2

    override protected def getDynamoDBDelegate(name: String): Table = getDynamoDBDelegate(name)
  }
}