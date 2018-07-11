package ink.baixin.ripple.query.rules

import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexSimplify
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.sql.`type`.SqlTypeUtil
import com.google.common.collect.ContiguousSet
import com.google.common.collect.DiscreteDomain
import ink.baixin.ripple.query.utils.FilterInference

import RelOptRule._

class RippleJoinFilterTranspose(relBuilderFactory: RelBuilderFactory) extends RelOptRule(
  operand(classOf[Filter],
    operand(classOf[Join], any())),
  relBuilderFactory, "RippleJoinFilterTranspose"
) {
  protected def perform(call: RelOptRuleCall, filter: Filter, join: Join) {
    if (join.getJoinType != JoinRelType.INNER) return

    val relBuilder = call.builder
    val rexBuilder = relBuilder.getRexBuilder
    val rexExecutor = call.getPlanner.getExecutor
    val rexSimplify = new RexSimplify(rexBuilder, false, rexExecutor)
    val joinFieldTypes = RelOptUtil.getFieldTypeList(join.getRowType)

    val jointFilters = scala.collection.JavaConverters.asScalaBuffer(
      RelOptUtil.conjunctions(rexSimplify.simplify(relBuilder.and(join.getCondition, filter.getCondition)))
    )
    val matchPairs = getMatchPairs(jointFilters).toSet
    val relatedFields = matchPairs.flatMap(p => Seq(p._1, p._2)).toSet.filter {
      (idx) => SqlTypeUtil.isIntType(joinFieldTypes.get(idx))
    }

    val inferredValues = relatedFields.map {
      (idx) =>
        val rset = FilterInference.infer[java.lang.Long](idx, jointFilters, 0L)
        if (!rset.isEmpty) {
          val dset = ContiguousSet.create(rset.span, DiscreteDomain.longs)
          if (dset.size == 1) Some(idx, dset.first)
          else None
        } else None
    }

    val inferredKV = inferredValues.flatMap {
      case Some((idx, v)) =>
        matchPairs.collect {
          case (a, b) if a == idx || b == idx => Seq((a, v), (b, v))
        } flatten
      case None => Seq()
    }

    val newFilters = inferredKV.collect {
      case (k, v) => relBuilder.equals(
        rexBuilder.makeInputRef(join, k),
        relBuilder.literal(v.asInstanceOf[java.lang.Long])
      )
    }

    val resFilters = rexSimplify.simplifyAnds(
      scala.collection.JavaConverters.asJavaIterable(jointFilters ++ newFilters)
    )

    call.transformTo(
      join.copy(join.getTraitSet, resFilters, join.getLeft, join.getRight, join.getJoinType, join.isSemiJoinDone)
    )
  }

  private def getInputRefIndex(node: RexNode) = {
    val ref = node match {
      case f if f.isA(SqlKind.CAST) => f.asInstanceOf[RexCall].getOperands.get(0)
      case f => f
    }
    if (ref.isInstanceOf[RexInputRef]) {
      Some(ref.asInstanceOf[RexInputRef].getIndex)
    } else None
  }

  private def getMatchPair(call: RexCall) = {
    (getInputRefIndex(call.getOperands.get(0)), getInputRefIndex(call.getOperands.get(1)))
  }

  private def getMatchPairs(nodes: Seq[RexNode]) = {
    nodes.collect {
      case node: RexCall if node.isA(SqlKind.EQUALS) => getMatchPair(node)
    } collect {
      case (Some(a), Some(b)) => (a, b)
    }
  }

  override def onMatch(call: RelOptRuleCall) {
    perform(call, call.rel(0), call.rel(1))
  }
}

object RippleJoinFilterTranspose {
  val INSTANCE = new RippleJoinFilterTranspose(RelFactories.LOGICAL_BUILDER)
}