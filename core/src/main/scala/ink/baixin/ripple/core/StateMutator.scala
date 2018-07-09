package ink.baixin.ripple.core

import org.joda.time.{DateTime, DateTimeZone}
import state._

trait StateMutator {

  protected def updateDelegate(uf: State => State): Option[State]

  def reserveIds(n: Long = 5): Seq[Long] = updateDelegate {
    old => old.withReservedId(old.reservedId + n)
  } match {
    case Some(ns) => ((ns.reservedId - n) until ns.reservedId)
    case _ => Seq()
  }

  def ensureSegments = updateDelegate { old =>
    // make sure we'll have enough segments in the future
    val today = DateTime.now(DateTimeZone.forID(old.timezone)).withTimeAtStartOfDay
    val safeTime = today.plusDays(2).getMillis
    val expireTime = today.minusDays(7).getMillis
    var newState = old
    if (newState.segments.isEmpty) {
      // at first we make sure this is a initial segment
      newState = newState
        .withReservedId(newState.reservedId + 1)
        .addSegments(
          State.Segment(
            newState.reservedId, true, false,
            today.minusDays(2).getMillis, today.plusDays(1).getMillis
          )
        )
    }
    while (newState.segments.last.endTime <= safeTime) {
      newState = newState
        .withReservedId(newState.reservedId + 1)
        .addSegments(
          State.Segment(
            newState.reservedId, true, false,
            newState.segments.last.endTime,
            new DateTime(newState.segments.last.endTime).plusDays(3).getMillis
          )
        )
    }

    // unprovision expired segments
    newState.withSegments(
      newState.segments.map { seg =>
        if (seg.endTime < expireTime) seg.withProvisioned(false)
        else seg
      }
    )
  }

  def addSegments(n: Int): Option[State] = updateDelegate { old =>
    val s = if (old.segments.isEmpty) {
      DateTime.now(DateTimeZone.forID(old.timezone)).withTimeAtStartOfDay
    } else {
      new DateTime(old.segments.last.endTime)
    }
    old
      .withReservedId(old.reservedId + n)
      .addAllSegments(
        for (i <- 0 until n) yield {
          State.Segment(
            old.reservedId + i, true, false,
            s.plusDays(i * 3).getMillis, s.plusDays(i * 3 + 3).getMillis
          )
        }
      )
  }

  def removeSegments(ids: Set[Long]) = updateDelegate {
    old => old.withSegments(old.segments.filterNot(s => ids.contains(s.id)))
  }

  def provisionSegments(ids: Set[Long], value: Boolean) = updateDelegate {
    old =>
      old.withSegments(old.segments.map {
        case seg if ids.contains(seg.id) => seg.withProvisioned(value)
        case seg => seg
      })
  }

  def aggregateSegments(ids: Set[Long], value: Boolean) = updateDelegate {
    old =>
      old.withSegments(old.segments.map {
        case seg if ids.contains(seg.id) => seg.withAggregated(value)
        case seg => seg
      })
  }

  def cleanUpSegments = updateDelegate {
    old => old.withSegments(old.segments.filter(s => s.provisioned || s.aggregated))
  }

  def clearSegments = updateDelegate(old => old.clearSegments)
}