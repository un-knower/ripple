package ink.baixin.ripple.core

import java.util.concurrent.TimeUnit

import com.amazonaws.services.dynamodbv2.document.Table
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import ink.baixin.ripple.core.documents.FactTable
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
          override def load(name: String): FactTable = {
            new FactTable(getDynamoDBDelegate(name))
          }
        }
      )
  }

  private def getSegmentTable(state: State, seg: State.Segment) =
    cache.get(s"${state.project}-${state.factTable}-${seg.id}")

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
    getStateDelegate match {
      case Some(state) => cache.get(s"${state.project}-${state.factTable}-users")
      case None =>
        syncAndGetStateDelegate match {
          case Some(state) => cache.get(s"${state.project}-${state.factTable}-users")
          case None => None
        }
    }
  }
}