package ink.baixin.ripple.query.rules

import org.apache.calcite.sql.SqlKind
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rel.core.SemiJoin
import org.apache.calcite.tools.RelBuilderFactory
import ink.baixin.ripple.query.rel.RippleTableScan
import RelOptRule._

class RippleJoinProjectTableScanRule(relBuilderFactory: RelBuilderFactory) extends RelOptRule(
  operand(classOf[SemiJoin],
    operand(classOf[RippleTableScan], none()), operand(classOf[Values], none())),
  relBuilderFactory, "RippleJoinProjectTableScanRule"
) {
  protected def perform(
                         call: RelOptRuleCall,
                         join: SemiJoin,
                         tableScan: RippleTableScan,
                         values: Values
                       ) {
    val tableNames = tableScan.getTable.getQualifiedName


    if (join.getJoinType != JoinRelType.INNER) return
    if (!Set("SESSIONS", "EVENTS").contains(tableNames.get(tableNames.size - 1))) return

    getMatchPair(join.getCondition) match {
      case (Some(a), Some(b)) if tableScan.fields(a).toLowerCase == "open_id" =>
        call.transformTo(
          new RippleTableScan(
            tableScan.getCluster,
            tableScan.getTable,
            tableScan.rippleTable,
            tableScan.allFields,
            tableScan.predicate.copy(openIds = Some(getOpenIds(values)))
          )
        )
      case _ => // do nothing
    }
  }

  private def getOpenIds(values: Values) = {
    scala.collection.JavaConverters.asScalaBuffer(values.getTuples).flatMap {
      case (row) if row.size == 1 =>
        scala.collection.JavaConverters.asScalaBuffer(row).collect {
          case (value) => value.getValue2.toString
        }
      case _ => Seq()
    } toSet
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

  private def getMatchPair(node: RexNode) = {
    node match {
      case call: RexCall if call.isA(SqlKind.EQUALS) =>
        (getInputRefIndex(call.getOperands.get(0)), getInputRefIndex(call.getOperands.get(1)))
      case _ => (None, None)
    }
  }

  private def getProjects(nodes: java.util.List[RexNode], fields: Seq[String]): Option[Seq[String]] = {
    val projectNames = for (i <- 0 until nodes.size) yield {
      nodes.get(i) match {
        case inputRef: RexInputRef =>
          val idx = inputRef.getIndex
          if (idx >= 0 && idx < fields.size) fields(idx)
          else return None
        case _ => return None
      }
    }
    Some(projectNames)
  }

  override def onMatch(call: RelOptRuleCall) {
    perform(call, call.rel(0), call.rel(1), call.rel(2))
  }
}

object RippleJoinProjectTableScanRule {
  val INSTANCE = new RippleJoinProjectTableScanRule(RelFactories.LOGICAL_BUILDER)
}