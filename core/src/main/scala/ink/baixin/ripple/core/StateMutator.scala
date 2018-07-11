package ink.baixin.ripple.core

import com.typesafe.scalalogging.Logger
import org.joda.time.{ DateTime, DateTimeZone }
import state._

trait StateMutator {
  private val logger = Logger(this.getClass.getName)

  protected def updateDelegate(uf: (State) => State): Option[State]

  def updateWatermark(ts: Long) = updateDelegate {
    (old) =>
      if (old.watermark >= ts) old
      else old.withWatermark(ts)
  }

  def reserveIds(n: Long = 5): Seq[Long] = updateDelegate {
    (old) => old.withReservedId(old.reservedId + n)
  } match {
    case Some(ns) => ((ns.reservedId - n) until ns.reservedId).toSeq
    case _ => Seq() // return empty range
  }

  def addSegments(n: Int): Option[State] = updateDelegate {
    (old) =>
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
              old.reservedId + i, false,
              s.plusDays(i * 3).getMillis, s.plusDays(i * 3 + 3).getMillis
            )
          }
        )
  }

  def removeSegments(ids: Set[Long]) = updateDelegate {
    (old) => old.withSegments(old.segments.filterNot(s => ids.contains(s.id)))
  }

  def enableSegments(ids: Set[Long]) = updateDelegate {
    (old) => old.withSegments(old.segments.map {
      case seg if ids.contains(seg.id) => seg.withEnabled(true)
      case seg => seg
    })
  }

  def disableSegments(ids: Set[Long]) =  updateDelegate {
    (old) => old.withSegments(old.segments.map {
      case seg if ids.contains(seg.id)  => seg.withEnabled(false)
      case seg => seg
    })
  }
}