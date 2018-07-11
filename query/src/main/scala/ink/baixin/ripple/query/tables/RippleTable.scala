package ink.baixin.ripple.query.tables

import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rel.RelNode
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.schema.Schemas
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.QueryableTable
import org.apache.calcite.schema.TranslatableTable
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.QueryProvider

object RippleTable {
  final case class Predicate(
                              appId: Option[Int] = None,
                              timeRange: Option[(Long, Long)] = None,
                              openIds: Option[Set[String]] = None,
                              fields: Option[Seq[String]] = None,
                              timeDescending: Option[Boolean] = None
                            )
}

trait RippleTable extends AbstractTable with TranslatableTable with QueryableTable {
  protected var rowType: RelDataType = null

  protected val fields = Seq[(String, SqlTypeName)]()
  protected val nullableFields = Set[String]()
  protected val fieldGroups = Map[String, Set[String]]()
  protected lazy val fieldKeys = fields.map(_._1)

  protected def getAttrs(fields: Seq[String]) = {
    val fns = fields.toSet
    fieldGroups.collect {
      case (name, group) if !group.intersect(fns).isEmpty => name
    } toSeq
  }

  protected def getRowType(tf: JavaTypeFactory) = {
    import scala.collection.JavaConversions.seqAsJavaList
    tf.createStructType(
      fields.map{
        case (n, t) if nullableFields.contains(n) => tf.createTypeWithNullability(tf.createSqlType(t), true)
        case (_, t) => tf.createSqlType(t)
      },
      fields.map(_._1),
    )
  }

  def getRowCount = 10E4

  override def getExpression(schema: SchemaPlus, tableName: String, clazz: Class[_]) = {
    Schemas.tableExpression(schema, getElementType(), tableName, clazz)
  }

  override def getRowType(tf: RelDataTypeFactory) = {
    if (rowType == null) rowType = getRowType(tf.asInstanceOf[JavaTypeFactory])
    rowType
  }

  override def getElementType() = classOf[RippleTable.Predicate]

  override def asQueryable[T](provider: QueryProvider, schema: SchemaPlus, tableName: String) = {
    throw new UnsupportedOperationException()
  }

  override def toRel(ctx: RelOptTable.ToRelContext, relOptTable: RelOptTable): RelNode = {
    import ink.baixin.ripple.query.rel.RippleTableScan
    new RippleTableScan(ctx.getCluster(), relOptTable, this, fieldKeys, RippleTable.Predicate())
  }

  def scan(ctx: DataContext, predicate: RippleTable.Predicate): Enumerable[AnyRef]
}