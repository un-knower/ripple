package ink.baixin.ripple.core

import com.amazonaws.services.dynamodbv2.document.Table
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import state._

trait ResourceResolver {
  protected def getStateDelegate: Option[State]
  protected def syncAndGetStateDelegate: Option[State]
  protected def getDynamoDBDelegate(name: String): Table

  private val cache: LoadingCache[String, Table] = {
    CacheBuilder.newBuilder()
      .maximumSize(100)
      .build(
        new CacheLoader[String, Table] {
          override def load(name: String): Table = {
            getDynamoDBDelegate(name)
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
    val table = getStateDelegate match {
      case Some(state) => cache.get(s"${state.project}-${state.factTable}-users")
      case None =>
        syncAndGetStateDelegate match {
          case Some(state) => cache.get(s"${state.project}-${state.factTable}-users")
          case None => None
        }
    }
  }
}