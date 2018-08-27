package ink.baixin.ripple.core

import scala.util.{Failure, Success, Try}
import java.util.concurrent.atomic.AtomicReference
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.spec._
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.util.TableUtils
import state._

class StateProvider(project: String, config: Config) {
  private val logger = Logger(this.getClass.getName)
  // use `AtomicReference` to insure atomic of state
  private val stateRef = new AtomicReference[(Long, Option[State])]((0, None))
  private val randomGen = new scala.util.Random()

  private lazy val dynamodbClient = {
    AmazonDynamoDBClientBuilder.standard()
      .withRegion(Regions.fromName(config.getString("aws-region")))
      .build()
  }

  // the table to preserve metadata in DynamoDB
  private lazy val metadataTable = {
    val tableName = config.getString("metadata-table")

    val tableSpec = (new CreateTableRequest())
      .withTableName(tableName)
      // set `project_name` as key schema, this is the partition key of the table
      .withKeySchema(new KeySchemaElement("project_name", KeyType.HASH))
      .withProvisionedThroughput(
        // set read and write time per second
        new ProvisionedThroughput()
          .withReadCapacityUnits(10)
          .withWriteCapacityUnits(5)
      )
      // set the table attribute's name and type
      .withAttributeDefinitions(
      new AttributeDefinition("project_name", ScalarAttributeType.S)
    )

    if (TableUtils.createTableIfNotExists(dynamodbClient, tableSpec)) {
      logger.info(s"event=created_metadata_table name=$tableName")
    }
    // wait for the table to be ready, at most for 300 seconds and check once per 5 seconds
    logger.debug(s"event=wait_metadata_table_active name=$tableName")
    TableUtils.waitUntilActive(dynamodbClient, tableName, 300000, 5000)

    (new DynamoDB(dynamodbClient)).getTable(tableName)
  }

  /**
    * Common function to update and get state atomically, use `stateRef.updateAndGet` as the parameter of `func`
    *
    * @param func
    * @return
    */
  private def compatUpdateAndGet(func: ((Long, Option[State])) => (Long, Option[State])) = {
    // for compatibility in scala 2.11
    import java.util.function.UnaryOperator
    stateRef.updateAndGet(new UnaryOperator[(Long, Option[State])] {
      override def apply(v: (Long, Option[State])) = func(v)
    })
  }

  private def syncState = compatUpdateAndGet { (old) =>
    logger.debug(s"event=fetch_state project=$project")
    Try(
      metadataTable.getItem(
        (new GetItemSpec)
          .withPrimaryKey("project_name", project)
          .withConsistentRead(true)
      )
    ) match {
      case Success(item: Item) => // get item from the table
        val id = item.getLong("id")
        val binary = item.getBinary("state")
        logger.debug(s"event=fetched_state project=$project id=$id")
        val newState = Some(State.parseFrom(binary))
        if ((id, newState) == old) {
          // state not changed, return the old tuple in case reference changed
          old
        } else {
          (id, newState)
        }
      case Success(_) => // table exists but have no items
        old // empty state, return old
      case Failure(e) => // get item fail
        logger.error(s"event=fetch_state_failure error=$e")
        // If failed, DynamoDB will retry, so first randomly sleep for a while then retry
        // to prevent read DynamoDB too frequently.
        Thread.sleep(randomGen.nextInt(2000) + 2000)
        // if sync failed, just return old state
        old
    }
  }

  private def updateState(uf: (Option[State]) => Option[State]): (Long, Option[State]) = {
    // retry at most 20 times
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

  private def internalUpdateState(uf: (Option[State]) => Option[State]) = compatUpdateAndGet { (old) =>
    logger.debug(s"event=update_state project=$project")
    val (oldId, oldState) = old
    val (newId, newState) = (System.currentTimeMillis, uf(oldState))

    if (newState.isEmpty || newState == oldState) {
      old // do nothing if newState is empty or not changed
    } else {

      val item = (new Item())
        .withPrimaryKey("project_name", project)
        .withNumber("id", newId)
        .withBinary("state", newState.get.toByteArray)

      val spec = if (oldState.isEmpty) {
        new PutItemSpec()
          .withItem(item)
          // make sure DynamoDB doesn't have this record
          .withConditionExpression("attribute_not_exists(project_name)")
      } else {
        new PutItemSpec()
          .withItem(item)
          // take inaccurate time clock into consideration, forbid two id to be too clear
          // here we enforce a 1 seconds gap between adjacent update id
          .withConditionExpression("attribute_exists(project_name) AND id = :oldId AND id < :limit")
          .withValueMap(
            new ValueMap()
              .withNumber(":oldId", oldId)
              .withNumber(":limit", newId - 1000)
          )
      }

      Try(metadataTable.putItem(spec)) match {
        case Success(_) =>
          // update succeed, return new state
          logger.debug(s"event=state_updated project=$project state=$newState")
          (newId, newState)
        case Failure(e) => {
          logger.error(s"event=state_put_failed project=$project state=$newState error=$e")
          throw e
        }
      }
    }
  }

  private def getNewState = {
    val timezone = config.getString("time-zone")
    State(
      project = project,
      timezone = timezone,
      reservedId = 5,
      segments = Seq()
    )
  }

  private def ensureUserTable(tableName: String) = Try {
    val tableSpec = (new CreateTableRequest())
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
          .withReadCapacityUnits(20)
          .withWriteCapacityUnits(10)
      )

    if (TableUtils.createTableIfNotExists(dynamodbClient, tableSpec)) {
      logger.info(s"event=created_user_table name=$tableName")
    }
    // wait for the table to be ready, at most for 300 seconds and check once per 5 seconds
    logger.debug(s"event=wait_user_table_active name=$tableName")
    TableUtils.waitUntilActive(dynamodbClient, tableName, 300000, 5000)
    (new DynamoDB(dynamodbClient)).getTable(tableName)
  }

  private def ensureFactTable(tableName: String) = Try {
    val tableSpec = (new CreateTableRequest())
      .withTableName(tableName)
      .withKeySchema(
        // store primary partition key on DynamoDB with HASH type
        new KeySchemaElement("aid", KeyType.HASH),
        // store primary sort key on DynamoDB with RANGE type
        new KeySchemaElement("sid", KeyType.RANGE)
      )
      .withAttributeDefinitions(
        new AttributeDefinition("aid", ScalarAttributeType.N),
        new AttributeDefinition("sid", ScalarAttributeType.B)
      )
      .withProvisionedThroughput(
        new ProvisionedThroughput()
          .withReadCapacityUnits(20)
          .withWriteCapacityUnits(10)
      )

    if (TableUtils.createTableIfNotExists(dynamodbClient, tableSpec)) {
      logger.info(s"event=created_fact_table name=$tableName")
    }
    // wait for the table to be ready, at most for 300 seconds and check once per 5 seconds
    logger.debug(s"event=wait_fact_table_active name=$tableName")
    TableUtils.waitUntilActive(dynamodbClient, tableName, 300000, 5000)

    // make DynamoDB's `Time to live attribute` enabled, after set this attribute,
    // there will exists a field in table to store the ttl timestamp
    try {
      val ttlSpec = new TimeToLiveSpecification()
        .withEnabled(true)
        .withAttributeName("expire")

      val ttlReq = new UpdateTimeToLiveRequest()
        .withTableName(tableName)
        .withTimeToLiveSpecification(ttlSpec)
      dynamodbClient.updateTimeToLive(ttlReq)
    } catch {
      case e: AmazonDynamoDBException =>
        logger.warn(s"event=enable_ttl_failed")
    }

    (new DynamoDB(dynamodbClient)).getTable(tableName)
  }

  private def ensureCountTable(tableName: String) = Try {
    val tableSpec = (new CreateTableRequest())
      .withTableName(tableName)
      .withKeySchema(
        new KeySchemaElement("aid", KeyType.HASH),
        new KeySchemaElement("sid", KeyType.RANGE)
      )
      .withAttributeDefinitions(
        new AttributeDefinition("aid", ScalarAttributeType.N),
        new AttributeDefinition("sid", ScalarAttributeType.S)
      )
      .withProvisionedThroughput(
        new ProvisionedThroughput()
          .withReadCapacityUnits(20)
          .withWriteCapacityUnits(10)
      )

    if (TableUtils.createTableIfNotExists(dynamodbClient, tableSpec)) {
      logger.info(s"event=created_count_table name=$tableName")
    }
    // wait for the table to be ready, at most for 300 seconds and check once per 5 seconds
    logger.debug(s"event=wait_count_table_active name=$tableName")
    TableUtils.waitUntilActive(dynamodbClient, tableName, 300000, 5000)

    try {
      val ttlSpec = new TimeToLiveSpecification()
        .withEnabled(true)
        .withAttributeName("expire")

      val ttlReq = new UpdateTimeToLiveRequest()
        .withTableName(tableName)
        .withTimeToLiveSpecification(ttlSpec)
      dynamodbClient.updateTimeToLive(ttlReq)
    } catch {
      case e: AmazonDynamoDBException =>
        logger.warn(s"event=enable_ttl_failed")
    }

    (new DynamoDB(dynamodbClient)).getTable(tableName)
  }

  private def ensureAggregationTable(tableName: String) = Try {
    val tableSpec = (new CreateTableRequest())
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
          .withReadCapacityUnits(20)
          .withWriteCapacityUnits(10)
      )

    if (TableUtils.createTableIfNotExists(dynamodbClient, tableSpec)) {
      logger.info(s"event=created_aggregation_table name=$tableName")
    }
    // wait for the table to be ready, at most for 300 seconds and check once per 5 seconds
    logger.debug(s"event=wait_aggregation_table_active name=$tableName")
    TableUtils.waitUntilActive(dynamodbClient, tableName, 300000, 5000)

    try {
      val ttlSpec = new TimeToLiveSpecification()
        .withEnabled(true)
        .withAttributeName("expire")

      val ttlReq = new UpdateTimeToLiveRequest()
        .withTableName(tableName)
        .withTimeToLiveSpecification(ttlSpec)
      dynamodbClient.updateTimeToLive(ttlReq)
    } catch {
      case e: AmazonDynamoDBException =>
        logger.warn(s"event=enable_ttl_failed")
    }

    (new DynamoDB(dynamodbClient)).getTable(tableName)
  }


  def ensureState = {
    // get state from DynamoDB
    syncState
    // update state to DynamoDB
    updateState { (old) =>
      old match {
        case None => Some(getNewState)
        case _ => old
      }
    }
    stateRef.get._2.isDefined
  }

  lazy val listener = new StateListener() {
    override def getState = stateRef.get._2

    override def syncAndGetState = syncState._2
  }

  lazy val mutator = new StateMutator() {
    override def updateDelegate(uf: (State) => State) = updateState {
      // only let mutator mutate an existing state
      case Some(old) => Some(uf(old))
      case _ =>
        logger.error(s"event=mutate_empty_state project=$project")
        None
    }._2
  }

  lazy val resolver = new ResourceResolver() {
    override def getStateDelegate = stateRef.get._2

    override def syncAndGetStateDelegate = syncState._2

    override def getFactTableDelegate(name: String): Table = ensureFactTable(name).get

    override def getUserTableDelegate(name: String): Table = ensureUserTable(name).get

    override def getCountTableDelegate(name: String): Table = ensureCountTable(name).get

    override def getAggregationTableDelegate(name: String): Table = ensureAggregationTable(name).get
  }
}