package ink.baixin.ripple.query

package object rules {
  import org.apache.calcite.plan.RelOptRule

  val ALL = Seq[RelOptRule](
    RippleFilterTableScanRule.INSTANCE,
    RippleProjectTableScanRule.INSTANCE,
    RippleJoinProjectTableScanRule.INSTANCE,
    RippleSortTableScanRule.INSTANCE,
  )
}
