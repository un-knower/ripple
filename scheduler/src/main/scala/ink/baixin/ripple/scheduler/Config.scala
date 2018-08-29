package ink.baixin.ripple.scheduler

import com.typesafe.config.ConfigFactory

class BaseConfig(section: String) {
  val baseConfig = ConfigFactory.load()
  val raw = if (baseConfig.hasPath(section)) {
    baseConfig.getConfig(section)
  } else {
    ConfigFactory.empty()
  }
}

object AWSConfig extends BaseConfig("aws") {
  lazy val region = {
    import awscala.Region
    if (raw.hasPath("region")) Region(raw.getString("region")) else Region.default()
  }
}

object MessageStorageConfig extends BaseConfig("message-storage") {
  val table = raw.getString("table")
}