package ink.baixin.ripple.scheduler
package executors

import ink.baixin.ripple.scheduler.services.{HiveService, MessageStorageService}
import org.joda.time.{DateTime, Days, Interval}

object HiveTableRefresh extends TaskExecutor {
  override def execute(msg: TaskMessage): Unit = {
    val cube = msg.data("cube")

    val start = DateTime.parse(msg.data("start"))
    val end = DateTime.parse(msg.data("end"))
    val interval = new Interval(start, end)
    // find formmer hive refresh task that is overlapped with current task
    val conflict = MessageStorageService.list.find {
      case ("finish", _, Some(task)) if (task.name == "hive_table_refresh" && task.data("cube") == cube) =>
        val taskInterval = new Interval(DateTime.parse(task.data("start")), DateTime.parse(task.data("end")))
        taskInterval.overlaps(interval)
      case _ => false
    }
    if (!conflict.isEmpty) {
      logger.warn(s"$this event=interval_conflict cube=$cube this_task=$msg that_task=${conflict.get}")
      throw new Exception(s"Task conflict: $msg and ${conflict.get}")
    }

    val hiveTables = TaskConfig.getHiveTables(cube)
    for (table <- hiveTables) {
      HiveService.withConnection { conn =>
        val statement = conn.createStatement()

        // config tez to be more parallely
        statement.execute("SET tex.grouping.min-size=500000")
        statement.execute("SET tez.grouping.max-size=1000000")
        statement.execute("SET hive.exec.max.dynamic.partitions=1000")
        statement.execute("SET hive.exec.max.dynamic.partitions.pernode=1000")
        statement.execute("SET hive.exec.dynamic.partition.mode=nonstrict")

        for (jar <- TaskConfig.getHiveAuxJars(table)) {
          statement.execute(s"ADD JAR $jar")
        }

        // update hive table partition
        val tableLocation = TaskConfig.getHiveTableLocation(table)
        if (tableLocation.isDefined) {
          // if table location is defined, manually add partitions with yyyy/mm/dd format
          // plus end with ten minutes for late events
          val days = Days.daysBetween(start, end.plusMinutes(10)).getDays
          val partitions = for (i <- 0 to days) yield {
            val t = start.plusDays(i)
            val location = s"${tableLocation.get}/${t.toString("yyyy/MM/dd")}"
            logger.info(s"$this event=add_partition location=$location")
            s"PARTITION (dt = '${t.toString("yyyy-MM-dd")}') LOCATION '$location'"
          }
          val sql = s"ALTER TABLE ${table}_json ADD IF NOT EXISTS ${partitions.mkString(" ")}"
          logger.info(s"$this event=add_table_partition statement=$sql")
          statement.execute(sql)
        } else {
          // if no table location is defined, assume default hive partition format and auto update
          logger.info(s"$this event=msck_repair_table table=$table")
          statement.execute(s"msck repair table ${table}_json")
        }

        logger.info(s"$this event=insert_data table=$table start=$start end=$end")
        statement.execute(
          s"""
            |INSERT INTO ${table}_parquet
            |PARTITION(dt)
            |SELECT * FROM ${table}_json
            |WHERE
            |dt >= '${start.toString("yyyy-MM-dd")}'
            |and dt <='${end.plusMinutes(10).toString("yyyy-MM-dd")}'
            |and `timestamp` >= '${start.toString("yyyy-MM-dd HH:mm:ss.S")}'
            |and `timestamp` < '${end.toString("yyyy-MM-dd HH:mm:ss.S")}'
          """.stripMargin)
      }
    }
  }
}