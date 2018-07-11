package ink.baixin.ripple.core

import java.util.concurrent.TimeUnit
import com.amazonaws.services.dynamodbv2.document.Table
import com.google.common.cache.{ LoadingCache, CacheLoader, CacheBuilder }
import state._
import documents._

trait ResourceResolver {

  protected def getStateDelegate: Option[State]
  protected def syncAndGetStateDelegate: Option[State]
  protected def getFactTableDelegate(name: String): Table
  protected def getUserTableDelegate(name: String): Table
  protected def getCountTableDelegate(name: String): Table
  protected def getAggregationTableDelegate(name: String): Table

  private val tableCache: LoadingCache[String, Option[Any]] = {
    CacheBuilder
      .newBuilder()
      .maximumSize(50)
      .expireAfterAccess(1, TimeUnit.DAYS)
      .build(
        new CacheLoader[String, Option[Any]]() {
          override def load(name: String): Option[Any] = {
            if (name.endsWith("fact")) {
              Some(new FactTable(getFactTableDelegate(name)))
            } else if (name.endsWith("users")) {
              Some(new UserTable(getUserTableDelegate(name)))
            } else if (name.endsWith("count")) {
              Some(new CountTable(getCountTableDelegate(name)))
            } else if (name.endsWith("agg")) {
              Some(new AggregationTable(getAggregationTableDelegate(name)))
            } else {
              None
            }
          }
        }
      )
  }

  def getFactTable =
    syncAndGetStateDelegate match {
      case Some(state) =>
        tableCache.get(getFactTableName(state)).collect {
          case e: FactTable => e
        }
      case None => None
    }

  def getUserTable =
    syncAndGetStateDelegate match {
      case Some(state) =>
        tableCache.get(getUserTableName(state)).collect {
          case e: UserTable => e
        }
      case None => None
    }

  def getCountTable =
    syncAndGetStateDelegate match {
      case Some(state) =>
        tableCache.get(getCountTableName(state)).collect {
          case e: CountTable => e
        }
      case None => None
    }

  def getAggregationTable =
    syncAndGetStateDelegate match {
      case Some(state) =>
        tableCache.get(getAggregationTableName(state)).collect {
          case e: AggregationTable => e
        }
      case None =>
        None
    }

  def getFactTableName(state: State) =
    s"ripple-${state.project}-fact"

  def getUserTableName(state: State) =
    s"ripple-${state.project}-users"

  def getCountTableName(state: State) =
    s"ripple-${state.project}-count"

  def getAggregationTableName(state: State) =
    s"ripple-${state.project}-agg"
}