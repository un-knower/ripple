package ink.baixin.ripple.query.rules

import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.core.Project
import org.apache.calcite.tools.RelBuilderFactory
import ink.baixin.ripple.query.rel.RippleTableScan
import RelOptRule._

class RippleProjectTableScanRule(relBuilderFactory: RelBuilderFactory) extends RelOptRule(
  operand(classOf[Project], operand(classOf[RippleTableScan], none())), relBuilderFactory, "RippleProjectTableScanRule"
) {
  protected def perform(call: RelOptRuleCall, project: Project, tableScan: RippleTableScan) {
    val projects = project.getProjects
    val fields = tableScan.fields

    getProjects(projects, fields).foreach {
      (names) =>
        call.transformTo(
          new RippleTableScan(
            tableScan.getCluster,
            tableScan.getTable,
            tableScan.rippleTable,
            tableScan.allFields,
            tableScan.predicate.copy(fields = Some(names))
          )
        )
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
    perform(call, call.rel(0), call.rel(1))
  }
}

object RippleProjectTableScanRule {
  val INSTANCE = new RippleProjectTableScanRule(RelFactories.LOGICAL_BUILDER)
}