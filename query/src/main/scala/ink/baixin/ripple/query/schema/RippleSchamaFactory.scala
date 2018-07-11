package ink.baixin.ripple.query.schema

import org.apache.calcite.schema.SchemaFactory
import org.apache.calcite.schema.SchemaPlus
import com.typesafe.config.ConfigFactory
import ink.baixin.ripple.core.StateProvider

class RippleSchemaFactory extends SchemaFactory {

  override def create(parent: SchemaPlus, name: String, mapConfig: java.util.Map[String, Object]) = {
    val config = ConfigFactory.load().getConfig("ripple")
    val sp = new StateProvider(name, config)
    sp.ensureState
    new RippleSchema(sp)
  }
}

object RippleSchemaFactory {
  val INSTANCE = new RippleSchemaFactory()
}