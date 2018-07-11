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

    val rexBuilder = call.builder.getRexBuilder
    val rexExecuter = call.getPlanner.getExecutor
    val rexSimplify = new RexSimplify(rexBuilder, false, rexExecuter)

    val condition = rexSimplify.simplify(filter.getCondition)

    val predicate = tableScan.predicate.copy(
      appId = mergeOption(inferAppId(appIdIndex, condition), tableScan.predicate.appId),
      openIds = mergeOption(inferOpenIds(openIdIndex, condition), tableScan.predicate.openIds),
      timeRange = mergeOption(inferTimestamp(timestampIndex, condition), tableScan.predicate.timeRange)
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

  private def inferTimestamp(idx: Int, node: RexNode) = {
    if (idx < 0) None
    else {
      val rangeset = FilterInference.infer[java.lang.Long](idx, Seq(node), 0L)
      if (!rangeset.isEmpty) {
        val range = rangeset.span
        val limit = Range.closedOpen[java.lang.Long](0L, Long.MaxValue)
        val safeRange = range.intersection(limit)

        val timestampSet = ContiguousSet.create(safeRange, DiscreteDomain.longs)
        Some(timestampSet.first.toLong, timestampSet.last.toLong)
      } else None
    }
  }
}

object RippleFilterTableScanRule {
  val INSTANCE = new RippleFilterTableScanRule(RelFactories.LOGICAL_BUILDER)
}