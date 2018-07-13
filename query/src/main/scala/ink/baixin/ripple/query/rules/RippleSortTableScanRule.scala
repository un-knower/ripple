package ink.baixin.ripple.query.rules

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.tools.RelBuilderFactory
import ink.baixin.ripple.query.rel.RippleTableScan

import RelOptRule._

class RippleSortTableScanRule(relBuilderFactory: RelBuilderFactory) extends RelOptRule(
  operand(classOf[Sort], operand(classOf[RippleTableScan], none())),
  relBuilderFactory, "RippleSortTableScanRule"
) {

  override def onMatch(call: RelOptRuleCall) {
    val sort: Sort = call.rel(0)
    val tableScan: RippleTableScan = call.rel(1)

    val tsIndex = tableScan.fields.indexOf("timestamp")

    if (tsIndex < 0) return

    val collations = scala.collection.JavaConverters.asScalaBuffer(sort.getCollation.getFieldCollations)
    val newTableScan = collations.find((col) => col.getFieldIndex == tsIndex).map {
      (tscol) =>
        new RippleTableScan(
          tableScan.getCluster,
          tableScan.getTable,
          tableScan.rippleTable,
          tableScan.allFields,
          tableScan.predicate.copy(timeDescending = Some(tscol.getDirection.isDescending))
        )
    }

    newTableScan.foreach {
      (nts) => call.transformTo(sort.copy(sort.getTraitSet, nts, sort.getCollation))
    }
  }
}

object RippleSortTableScanRule {
  val INSTANCE = new RippleSortTableScanRule(RelFactories.LOGICAL_BUILDER)
}