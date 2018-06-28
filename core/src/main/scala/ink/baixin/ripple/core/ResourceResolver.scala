package ink.baixin.ripple.core

import java.util.concurrent.TimeUnit

import com.amazonaws.services.dynamodbv2.document.Table
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import ink.baixin.ripple.core.documents.{FactTable, UserTable}
import state._

trait ResourceResolver {
  protected def getStateDelegate: Option[State]

  protected def syncAndGetStateDelegate: Option[State]

  protected def getDynamoDBDelegate(name: String): Table

  private val cache: LoadingCache[String, FactTable] = {
    CacheBuilder
      .newBuilder()
      .maximumSize(100)
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(
        new CacheLoader[String, FactTable] {
          override def load(name: String): FactTable =
            new FactTable(getDynamoDBDelegate(name))
        }
      )
  }

  private val userTableCache: LoadingCache[String, UserTable] = {
    CacheBuilder
      .newBuilder()
      .maximumSize(5)
      .expireAfterAccess(1, TimeUnit.DAYS)
      .build(
        new CacheLoader[String, UserTable] {
          override def load(name: String): UserTable =
            new UserTable(getDynamoDBDelegate(name))
        }
      )
  }

  private def getSegmentTable(state: State, seg: State.Segment) =
    cache.get(getFactTableName(state, seg))

  def getFactTable(ts: Long) = {
    val table = getStateDelegate match {
      case Some(state) =>
        state.segments
          .find(s => s.startTime <= ts && ts < s.endTime)
          .map(s => getSegmentTable(state, s))
      case None => None
    }

    if (table.isDefined) table
    else syncAndGetStateDelegate match {
      case Some(state) =>
        state.segments
          .find(s => s.startTime <= ts && ts < s.endTime)
          .map(s => getSegmentTable(state, s))
      case None => None
    }
  }

  def getUserTable = {
    val table = getStateDelegate match {
      case Some(state) => Some(cache.get(getUserTableName(state)))
      case None => None
    }
    if (table.isDefined) table
    else syncAndGetStateDelegate match {
      case Some(state) => Some(cache.get(getUserTableName(state)))
      case None => None
    }
  }

  def getFactTableName(state: State, seg: State.Segment) =
    s"${state.project}-${state.factTable}-${seg.id}"

  def getUserTableName(state: State) =
    s"${state.project}-${state.factTable}-user"
}