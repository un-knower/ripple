package ink.baixin.ripple.query.rules

import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexSimplify
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.tools.RelBuilderFactory
import com.google.common.collect.Range
import com.google.common.collect.ContiguousSet
import com.google.common.collect.DiscreteDomain
import ink.baixin.ripple.query.rel.RippleTableScan
import ink.baixin.ripple.query.utils.FilterInference
import RelOptRule._
import ink.baixin.ripple.query.tables.RippleExprSimplifier

class RippleFilterTableScanRule(relBuilderFactory: RelBuilderFactory) extends RelOptRule(
  operand(classOf[Filter], operand(classOf[RippleTableScan], none())),
  relBuilderFactory, "RippleFilterTableScanRule"
) {

  override def onMatch(call: RelOptRuleCall) {
    val filter: Filter = call.rel(0)
    val tableScan: RippleTableScan = call.rel(1)

    val appIdIndex = tableScan.fields.indexOf("app_id")
    val openIdIndex = tableScan.fields.indexOf("open_id")
    val timestampIndex = tableScan.fields.indexOf("timestamp")
    val cstDtIndex = tableScan.fields.indexOf("cst_date")

    val rexBuilder = call.builder.getRexBuilder
    val rexExecuter = call.getPlanner.getExecutor
    val rexSimplify = new RexSimplify(rexBuilder, false, rexExecuter)
    val exprSimplifier = new RippleExprSimplifier(rexSimplify, false)

    val condition = exprSimplifier.apply(filter.getCondition)

    val predicate = tableScan.predicate.copy(
      appId = mergeOption(inferAppId(appIdIndex, condition), tableScan.predicate.appId),
      openIds = mergeOption(inferOpenIds(openIdIndex, condition), tableScan.predicate.openIds),
      timeRange = mergeOption(inferTimestamp(timestampIndex, cstDtIndex, condition), tableScan.predicate.timeRange)
    )

    val pushable = new java.util.ArrayList[RexNode]()
    val notPushable = new java.util.ArrayList[RexNode]()
    val bitmap = if (appIdIndex >= 0 && openIdIndex >= 0 && predicate.appId.isDefined && predicate.openIds.isDefined) {
      ImmutableBitSet.of(appIdIndex, openIdIndex)
    } else if (appIdIndex >= 0 && predicate.appId.isDefined) {
      ImmutableBitSet.of(appIdIndex)
    } else {
      ImmutableBitSet.of()
    }

    RelOptUtil.splitFilters(bitmap, condition, pushable, notPushable)

    call.transformTo(
      RelOptUtil.createFilter(
        new RippleTableScan(
          tableScan.getCluster,
          tableScan.getTable,
          tableScan.rippleTable,
          tableScan.allFields,
          predicate),
        notPushable
      )
    )
  }

  private def mergeOption[T](a: Option[T], b: Option[T]) = {
    if (a.isDefined) a
    else b
  }

  private def inferAppId(idx: Int, node: RexNode) = {
    val rangeset = FilterInference.infer[java.lang.Long](idx, Seq(node), 0L)
    if (!rangeset.isEmpty) {
      val range = rangeset.span
      val appIdSet = ContiguousSet.create(range, DiscreteDomain.longs)
      if (appIdSet.size == 1) {
        Some(appIdSet.first().toInt)
      } else {
        None
      }
    } else None
  }

  private def inferOpenIds(idx: Int, node: RexNode) = {
    FilterInference.inferSet[java.lang.String](idx, Seq(node), "") match {
      case res @ Some(openIds) if !openIds.isEmpty => res
      case _ => None
    }
  }

  private def inferTimestamp(tsIdx: Int, cstDtIdx: Int, node: RexNode) = {
    val tsLimit = Range.closedOpen[java.lang.Long](0L, Long.MaxValue)
    val dtLimit = Range.closedOpen[java.lang.Integer](0, Int.MaxValue)

    val tsRange =
      if (tsIdx < 0) tsLimit
      else FilterInference.infer[java.lang.Long](tsIdx, Seq(node), 0L).span.intersection(tsLimit)

    val dtRange =
      if (cstDtIdx < 0) tsLimit
      else {
        val dt = FilterInference.infer[java.lang.Integer](cstDtIdx, Seq(node), 0).span.intersection(dtLimit)
        Range.range[java.lang.Long](
          (dt.lowerEndpoint().toLong * 24 - 8) * 3600 * 1000, dt.lowerBoundType,
          (dt.upperEndpoint().toLong * 24 + 16) * 3600 * 1000, dt.upperBoundType
        )
      }

    val range = tsRange.intersection(dtRange)
    val tsSet = ContiguousSet.create(range, DiscreteDomain.longs)
    if (tsSet.isEmpty) Some(0L, 0L)
    else Some(tsSet.first.toLong, tsSet.last.toLong)
  }
}

object RippleFilterTableScanRule {
  val INSTANCE = new RippleFilterTableScanRule(RelFactories.LOGICAL_BUILDER)
}