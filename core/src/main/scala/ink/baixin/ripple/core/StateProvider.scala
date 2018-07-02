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
  private val logger = Logger(this.getClass)
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
      .withProvisionedThroughput(
        new ProvisionedThroughput()
          .withReadCapacityUnits(10L)
          .withWriteCapacityUnits(5L)
      )
      .withAttributeDefinitions(
        // string type
        new AttributeDefinition("project_name", ScalarAttributeType.S),
      )

    if (TableUtils.createTableIfNotExists(dynamodbClient, tableSpec)) {
      logger.info(s"event=create_metadata_table name=$tableName")
    }
    // wait for the table to be ready, at most 300 seconds and check once per 5 seconds
    logger.debug(s"event=wait_metadata_table_active name=$tableName")
    TableUtils.waitUntilActive(dynamodbClient, tableName, 300 * 1000, 5 * 1000)

    new DynamoDB(dynamodbClient).getTable(tableName)
  }

  private def syncState = stateRef.updateAndGet { old =>
    logger.debug(s"event=fetch_state project=$project")
    Try(
      metadataTable.getItem(new GetItemSpec()
        .withPrimaryKey("project_name", project)
        .withConsistentRead(true))
    ) match {
      case Success(item: Item) =>
        val id = item.getLong("id")
        val binary = item.getBinary("state")
        logger.debug(s"event=fetched_state project=$project id=$id")
        val newState = Some(State.parseFrom(binary))
        if ((id, newState) == old) {
          // state not changed, return the old tuple
          old
        } else {
          (id, newState)
        }
      case Success(_) =>
        old // empty state, return old
      case Failure(e) =>
        logger.error(s"event=fetch_state_failure error=$e")
        Thread.sleep(randomGen.nextInt(2000) + 2000) // randomly sleep for a while
        // if sync failed, just return old state
        old
    }
  }

  private def updateState(uf: Option[State] => Option[State]): (Long, Option[State]) = {
    var retries = 20
    while (retries > 0) {
      try {
        syncState
        return internalUpdateState(uf)
      } catch {
        case e: Exception =>
          logger.error(s"event=failed_to_update_state project=$project error=$e")
          Thread.sleep(randomGen.nextInt(1000) + 1000) // randomly sleep for a while
      } finally {
        retries -= 1
      }
    }
    stateRef.get
  }

  private def internalUpdateState(uf: Option[State] => Option[State]) =
    stateRef.updateAndGet { old =>
      val (oldId, oldState) = old
      val (newId, newState) = (System.currentTimeMillis, uf(oldState))

      if (newState.isEmpty || newState == oldState) {
        old // do nothing if newState is empty or not changed
      } else {
        val item = new Item()
          .withPrimaryKey("project_name", project)
          .withNumber("id", newId)
          .withBinary("state", newState.get.toByteArray)

        // set filter condition
        val spec = if (oldState.isEmpty) {
          new PutItemSpec()
            .withItem(item)
            .withConditionExpression("attribute_not_exists(project_name)")
        } else {
          new PutItemSpec()
            .withItem(item)
            // take inaccurate time clock into consideration, forbid two id to be clear
            // here we enfore a 1 second gap between adjacent update id
            .withConditionExpression("attribute_exists(project_name) AND id = :oldId and id < :limit")
            .withValueMap(new ValueMap()
              .withNumber(":oldId", oldId)
              .withNumber(":limit", newId - 1000))
        }

        Try(metadataTable.putItem(spec)) match {
          case Success(_) =>
            // update succeed, return new state
            logger.debug(s"event=state_updated project=$project state=$newState")
            (newId, newState)
          case Failure(e) =>
            logger.error(s"event=state_put_failed project=$project state=$newState error=$e")
            throw e
        }
      }
    }

  private def getNewState = {
    val timezone = config.getString("time-zone")
    State(
      project = project,
      timezone = timezone,
      factTable = config.getString("fact-table"),
      resourceBucket = config.getString("resource-bucket"),
      reservedId = 5, // TODO why set default reservedId to 5
      segments = Seq()
    )
  }

  def ensureUserTable(tableName: String) = Try {
    val tableSpec = new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(
        new KeySchemaElement("aid", KeyType.HASH),
        new KeySchemaElement("oid", KeyType.RANGE)
      )
      .withAttributeDefinitions(
        new AttributeDefinition("aid", ScalarAttributeType.N),
        new AttributeDefinition("oid", ScalarAttributeType.S)
      )
      .withProvisionedThroughput(
        new ProvisionedThroughput()
          .withReadCapacityUnits(10L)
          .withWriteCapacityUnits(5L)
      )

    if (TableUtils.createTableIfNotExists(dynamodbClient, tableSpec)) {
      logger.info(s"event=created_user_table name=$tableName")
    }
    logger.debug(s"event=wait_user_table_active name=$tableName")
    TableUtils.waitUntilActive(dynamodbClient, tableName, 300 * 1000, 5 * 1000)

    new DynamoDB(dynamodbClient).getTable(tableName)
  }

  def ensureFactTable(tableName: String) = Try {
    val tableSpec = new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(
        new KeySchemaElement("aid", KeyType.HASH),
        new KeySchemaElement("sid", KeyType.RANGE)
      )
      .withAttributeDefinitions(
        new AttributeDefinition("aid", ScalarAttributeType.N),
        new AttributeDefinition("sid", ScalarAttributeType.B)
      )
      .withProvisionedThroughput(
        new ProvisionedThroughput()
          .withReadCapacityUnits(10L)
          .withWriteCapacityUnits(5L)
      )

    if (TableUtils.createTableIfNotExists(dynamodbClient, tableSpec)) {
      logger.info(s"event=created_fact_table name=$tableName")
    }
    logger.debug(s"event=wait_fact_table_active name=$tableName")
    TableUtils.waitUntilActive(dynamodbClient, tableName, 300 * 1000, 5 * 1000)

    new DynamoDB(dynamodbClient).getTable(tableName)
  }

  def ensureState = {
    updateState { old =>
      old match {
        case None => Some(getNewState)
        case _ => old
      }
    }
    stateRef.get._2.isDefined
  }

  lazy val listener = new StateListener {
    override val listenInterval =
      if (config.hasPath("listen-internal")) config.getInt("listen-internal") else 30 * 1000

    override def getState: Option[State] = stateRef.get._2

    override def syncAndGetState: Option[State] = syncState._2
  }

  lazy val mutator = new StateMutator {
    override def updateDelegate(uf: State => State): Option[State] = updateState {
      // only let mutator mutate an existing state
      case Some(old) => Some(uf(old))
      case _ =>
        logger.error(s"event=mutate_empty_state project=$project")
        None
    }._2
  }

  lazy val resolver = new ResourceResolver {
    override protected def syncAndGetStateDelegate: Option[State] = stateRef.get._2

    override protected def getStateDelegate: Option[State] = syncState._2

    override def getUserTableDelegate(name: String): Table = ensureUserTable(name).get

    override def getFactTableDelegate(name: String): Table = ensureFactTable(name).get
  }
}