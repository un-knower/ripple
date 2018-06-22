package ink.baixin.ripple.core

import org.joda.time.DateTime
import state._

trait StateMutator {

  protected def updateDelegate(uf: State => State): Option[State]

  def reserveIds(n: Long = 5): Seq[Long] = updateDelegate {
    old => old.withReservedId(old.reservedId + n)
  } match {
    case Some(ns) => ((ns.reservedId - n) until ns.reservedId)
    case _ => Seq()
  }

  def addSegments(ids: Seq[Long]): Option[State] = updateDelegate {
    old =>
      val s = new DateTime(old.segments.last.startTime)
      val segs = ids.zipWithIndex.collect {
        case (id, i) => State.Segment(id, false, false,
          s.plusDays(i * 3).getMillis,
          s.plusDays(i * 3 + 3).getMillis)
      }
      old.withReservedId(old.reservedId + 1).addAllSegments(segs)
  }

  def addSegments(n: Int): Option[State] = {
    addSegments(reserveIds(n))
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
}