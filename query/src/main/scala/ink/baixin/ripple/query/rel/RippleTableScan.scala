package ink.baixin.ripple.query.rel

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.adapter.enumerable.PhysTypeImpl
import org.apache.calcite.adapter.enumerable.EnumerableRel
import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor
import org.apache.calcite.linq4j.tree.Blocks
import org.apache.calcite.linq4j.tree.Expressions
import org.apache.calcite.rel.metadata.RelMetadataQuery
import ink.baixin.ripple.query.rules
import ink.baixin.ripple.query.tables.RippleTable

object RippleTableScan {
  def getCollationTrait(allFields: Seq[String], predicate: RippleTable.Predicate) = {
    val fields = predicate.fields.getOrElse(allFields)
    if (predicate.appId.isDefined && fields.indexOf("timestamp") >= 0) {
      val fieldCollation = predicate.timeDescending match {
        case Some(false) =>
          new RelFieldCollation(fields.indexOf("timestamp"), RelFieldCollation.Direction.ASCENDING)
        case _ =>
          new RelFieldCollation(fields.indexOf("timestamp"), RelFieldCollation.Direction.DESCENDING)
      }
      RelCollations.of(fieldCollation)
    } else {
      RelCollations.EMPTY
    }
  }
}


class RippleTableScan(
                     cluster: RelOptCluster, table: RelOptTable,
                     val rippleTable: RippleTable, val allFields: Seq[String], val predicate: RippleTable.Predicate
                   ) extends TableScan(cluster, cluster.traitSetOf(
  EnumerableConvention.INSTANCE, RippleTableScan.getCollationTrait(allFields, predicate)
), table) with EnumerableRel {

  val fields = predicate.fields match {
    case Some(f) => f
    case None => allFields
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]) =
    new RippleTableScan(cluster, table, rippleTable, allFields, predicate)

  override def explainTerms(pw: RelWriter) =
    super.explainTerms(pw).item("fields", predicate)

  override def deriveRowType() = {
    val fieldList = table.getRowType().getFieldList()
    val builder = cluster.getTypeFactory.builder

    for (field <- fields) {
      builder.add(fieldList.get(allFields.indexOf(field)))
    }
    builder.build
  }

  override def estimateRowCount(mq: RelMetadataQuery) = {
    table.getRowCount
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery) = {
    var rows = estimateRowCount(mq)
    if (predicate.appId.isDefined) rows /= 100.0
    if (predicate.openIds.isDefined) rows /= 5.0
    predicate.timeRange.foreach {
      case (a, b) =>
        if (a > 0 && b < (Long.MaxValue >> 1)) rows /= 3.0
        else if (a > 0 || b < (Long.MaxValue >> 1)) rows /= 2.0
    }
    planner.getCostFactory.makeCost(rows, 0.0, 0.0)
  }


  override def register(planner: RelOptPlanner) {
    rules.ALL.foreach {
      (rule) => planner.addRule(rule)
    }
  }

  override def implement(implementor: EnumerableRelImplementor, pref: EnumerableRel.Prefer) = {
    val physType = PhysTypeImpl.of(
      implementor.getTypeFactory(),
      getRowType(),
      pref.preferArray()
    )

    implementor.result(
      physType,
      Blocks.toBlock(
        Expressions.call(
          table.getExpression(rippleTable.getClass), "scan",
          implementor.getRootExpression, implementor.stash(predicate, classOf[RippleTable.Predicate])
        )
      )
    )
  }
}