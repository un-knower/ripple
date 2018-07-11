package ink.baixin.ripple.query.utils

import com.google.common.collect.Range
import com.google.common.collect.RangeSet
import com.google.common.collect.BoundType
import com.google.common.collect.ImmutableRangeSet
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.sql.SqlKind
import scala.collection.JavaConversions._

object FilterInference {

  private def getInputRefIndex(node: RexNode) = {
    val ref = node match {
      case f if f.isA(SqlKind.CAST) => f.asInstanceOf[RexCall].getOperands.get(0)
      case f => f
    }
    if (ref.isInstanceOf[RexInputRef]) {
      Some(ref.asInstanceOf[RexInputRef].getIndex)
    } else None
  }

  private def getLiteral[T](node: RexNode) = node match {
    case v: RexLiteral => Some(v.getValue2.asInstanceOf[T])
    case _ => None
  }

  private def getInputLiteralPair[T](call: RexCall) = {
    (getInputRefIndex(call.getOperands.get(0)), getLiteral[T](call.getOperands.get(1)))
  }

  private def inferField[T <: Comparable[T]](index: Int, node: RexNode, empty: T): RangeSet[T] = {
    if (node.isA(SqlKind.EQUALS)) {
      getInputLiteralPair[T](node.asInstanceOf[RexCall]) match {
        case (Some(idx), Some(v)) if idx == index =>
          ImmutableRangeSet.of(Range.closed(v, v))
        case _ => ImmutableRangeSet.of(Range.all[T]())
      }
    } else if (node.isA(SqlKind.NOT_EQUALS)) {
      getInputLiteralPair[T](node.asInstanceOf[RexCall]) match {
        case (Some(idx), Some(v)) if idx == index =>
          ImmutableRangeSet.of(Range.closed(v, v)).complement
        case _ => ImmutableRangeSet.of(Range.all[T]())
      }
    } else if (node.isA(SqlKind.LESS_THAN)) {
      getInputLiteralPair[T](node.asInstanceOf[RexCall]) match {
        case (Some(idx), Some(v)) if idx == index =>
          ImmutableRangeSet.of(Range.lessThan(v))
        case _ => ImmutableRangeSet.of(Range.all[T]())
      }
    } else if (node.isA(SqlKind.GREATER_THAN)) {
      getInputLiteralPair[T](node.asInstanceOf[RexCall]) match {
        case (Some(idx), Some(v)) if idx == index =>
          ImmutableRangeSet.of(Range.greaterThan[T](v))
        case _ => ImmutableRangeSet.of(Range.all[T]())
      }
    } else if (node.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
      getInputLiteralPair[T](node.asInstanceOf[RexCall]) match {
        case (Some(idx), Some(v)) if idx == index =>
          ImmutableRangeSet.of(Range.atMost(v))
        case _ => ImmutableRangeSet.of(Range.all[T]())
      }
    } else if (node.isA(SqlKind.GREATER_THAN_OR_EQUAL)) {
      getInputLiteralPair[T](node.asInstanceOf[RexCall]) match {
        case (Some(idx), Some(v)) if idx == index =>
          ImmutableRangeSet.of(Range.atLeast(v))
        case _ => ImmutableRangeSet.of(Range.all[T]())
      }
    } else if (node.isAlwaysFalse) {
      ImmutableRangeSet.of(Range.openClosed[T](empty, empty))
    } else ImmutableRangeSet.of(Range.all[T]())
  }

  private def intersectRangeSet[T <: Comparable[T]](a: RangeSet[T], b: RangeSet[T]) = {
    if (a.complement.isEmpty) b
    else if (b.complement.isEmpty) a
    else ImmutableRangeSet.copyOf[T](a).intersection(b)
  }

  private def unionRangeSet[T <: Comparable[T]](a: RangeSet[T], b: RangeSet[T]) = {
    if (a.complement.isEmpty) a
    else if (b.complement.isEmpty) b
    else ImmutableRangeSet.copyOf[T](a).union(b)
  }

  private def infer[T <: Comparable[T]](index: Int, node: RexNode, empty: T): RangeSet[T] = {
    if (node.isA(SqlKind.AND)) {
      val call = node.asInstanceOf[RexCall]
      call.getOperands()
        .map(n => infer[T](index, n, empty))
        .reduce((a, b) => intersectRangeSet(a, b))
    } else if (node.isA(SqlKind.OR)) {
      val call = node.asInstanceOf[RexCall]
      call.getOperands()
        .map((n) => infer[T](index, n, empty))
        .reduce((a, b) => unionRangeSet(a, b))
    } else if (node.isA(SqlKind.NOT)) {
      val call = node.asInstanceOf[RexCall]
      call.getOperands()
        .map((n) => infer[T](index, n, empty))
        .reduce((a, b) => unionRangeSet(a, b))
        .complement
    } else if (node.isAlwaysFalse) {
      ImmutableRangeSet.of(Range.openClosed[T](empty, empty))
    } else {
      inferField(index, node, empty)
    }
  }

  def infer[T <: Comparable[T]](index: Int, filters: Seq[RexNode], empty: T): RangeSet[T] = {
    filters
      .map((node) => infer[T](index, node, empty))
      .reduce {
        (a, b) => intersectRangeSet(a, b)
      }
  }

  def inferSet[T <: Comparable[T]](index: Int, filters: Seq[RexNode], empty: T): Option[Set[T]] = {
    val rangeset = infer[T](index, filters, empty)
    val ranges = rangeset.asRanges
    if (ranges.forall((r) => r.hasLowerBound && r.hasUpperBound && r.lowerEndpoint == r.upperEndpoint)) {
      // Every range in range set contains at most one element
      val res = ranges.collect {
        case r if r.upperBoundType == BoundType.CLOSED && r.lowerBoundType == BoundType.CLOSED =>
          r.lowerEndpoint
      }
      Some(res.toSet)
    } else None
  }
}