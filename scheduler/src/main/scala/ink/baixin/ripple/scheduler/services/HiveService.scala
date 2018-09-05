package ink.baixin.ripple.scheduler.services

import java.sql.{Connection, DriverManager}

import ink.baixin.ripple.scheduler.HiveConfig

object HiveService {
  def connection = DriverManager.getConnection(HiveConfig.url, HiveConfig.username, HiveConfig.password)

  def withConnection(f: Connection => Unit): Unit = {
    val conn = connection
    try {
      f(conn)
    } finally {
      conn.close()
    }
  }
}
