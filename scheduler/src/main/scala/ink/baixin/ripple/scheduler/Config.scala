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

object TaskConfig extends BaseConfig("task") {
  import collection.JavaConverters._

  val wmpTask = if (raw.hasPath("wmp_task")) {
    val wmpInfo = raw.getConfig("wmp_task")
    Some(Map(
      "name" -> wmpInfo.getString("spark_job_name"),
      "table" -> wmpInfo.getString("table"),
      "user-table" -> wmpInfo.getString("user_table"),
      "event-table" -> wmpInfo.getString("event_table"),
      "openid-table" -> wmpInfo.getString("openid_table"),
      "session-table" -> wmpInfo.getString("session_table"),
      "warehouse" -> wmpInfo.getString("warehouse")
    ))
  } else None

  // refresh times per day
  val wmpRefreshRate = if (raw.hasPath("wmp_refresh_rate")) raw.getInt("wmp_refresh_rate") else 4
  // refresh interval of hour
  val wmpRefreshInterval = 24 / wmpRefreshRate

  val kylinCubes = raw.getStringList("kylin_cubes").asScala
  val kylinProject = raw.getString("kylin_project")
}

object HadoopConfig extends BaseConfig("hadoop") {
  val user = raw.getString("user")
  val defaultFS = raw.getString("default_fs")
  val yarnRNAddress = raw.getString("yarn_rn_address")
}

object SparkConfig extends BaseConfig("spark") {
  val eventLogEnabled = raw.getString("eventlog_enabled")
  val eventLogDir = raw.getString("eventlog_dir")
  val historyDir = raw.getString("history_dir")
  val appJar = raw.getString("app_jar")
  val sparkLibsArchive = raw.getString("spark_libs_archive")
}