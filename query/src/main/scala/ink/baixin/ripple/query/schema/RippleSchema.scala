package ink.baixin.ripple.query.schema

import com.google.common.collect.ImmutableMap

import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema

import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.query.tables._

class RippleSchema(private val sp: StateProvider) extends AbstractSchema {

  private lazy val tableMap = {
    val builder = ImmutableMap.builder[String, Table]()
    builder.put("SESSIONS", new SessionScanTable(sp))
    builder.put("EVENTS", new EventScanTable(sp))
    builder.put("USERS", new UserScanTable(sp))
    builder.put("COUNTS", new CountScanTable(sp))
    builder.put("AGGREGATIONS", new AggregationScanTable(sp))
    builder.build()
  }

  override protected def isMutable() = false
  override protected def getTableMap() = tableMap
}