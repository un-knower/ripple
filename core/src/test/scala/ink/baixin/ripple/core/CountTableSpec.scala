package ink.baixin.ripple.core

import org.scalatest._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import models._

class CountTableSpec extends FlatSpec {
  private val logger = Logger(this.getClass)
  private val testConfig = ConfigFactory.load().getConfig("ripple-test")

  private lazy val table = {
    val provider = new StateProvider("local-test", testConfig)
    val state = provider.ensureState
    provider.resolver.getCountTable.get
  }

  private val countRecords = for (i <- 0 until 1000) yield {
    CountRecord(
      i % 7,
      s"open-id-${i % 13}",
      (if (i % 2 == 0) "ui" else "pv"),
      s"event-key-${i % 17}",
      0,
      1529484567797L + i.toLong
    )
  }

  it should "get count successfully" in {
    for (c <- countRecords) {
      val res = table.increaseCount(c.appId, c.openId, c.eventType, c.eventKey, c.timestamp)
      assert(res.get.count == 1)
    }
  }
}