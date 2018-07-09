package ink.baixin.ripple.core

import java.util.concurrent.TimeUnit
import com.amazonaws.services.dynamodbv2.document.Table
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import ink.baixin.ripple.core.documents.{SegmentTable, UserTable}
import state._

trait ResourceResolver {
  protected def getStateDelegate: Option[State]

  protected def syncAndGetStateDelegate: Option[State]

  protected def getUserTableDelegate(name: String): Table

  protected def getSegmentTableDelegate(name: String): Table

  private val segmentTableCache: LoadingCache[String, SegmentTable] = {
    CacheBuilder
      .newBuilder()
      .maximumSize(50)
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(
        new CacheLoader[String, SegmentTable] {
          override def load(name: String): SegmentTable =
            new SegmentTable(getSegmentTableDelegate(name))
        }
      )
  }

  def getSegmentTables =
    syncAndGetStateDelegate match {
      case Some(state) =>
        state.segments.collect {
          case seg if seg.provisioned =>
            val name = getSegmentTableName(state, seg)
            val table = segmentTableCache.get(name)
            (seg, table)
        }
      case None =>
        Seq()
    }

  def getUserTable =
    syncAndGetStateDelegate match {
      case Some(state) =>
        val name = getUserTableName(state)
        Some(new UserTable(getUserTableDelegate(name)))
      case None =>
        None
    }

  def getSegmentTableName(state: State, seg: State.Segment) =
    s"${state.project}-${state.factTable}-${seg.id}"

  def getUserTableName(state: State) =
    s"${state.project}-${state.factTable}-users"
}