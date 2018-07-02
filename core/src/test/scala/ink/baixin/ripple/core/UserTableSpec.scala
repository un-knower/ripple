package ink.baixin.ripple.core

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.models.User
import org.scalatest.FlatSpec

class UserTableSpec extends FlatSpec {
  private val logger = Logger(this.getClass)
  private val testConfig = ConfigFactory.load.getConfig("ripple-test")

  private lazy val table = {
    val provider = new StateProvider("test-project", testConfig)
    provider.ensureState
    provider.resolver.getUserTable.get
  }

  private val users = for (i <- 0 until 100) yield {
    val u = User()
      .withAppId(i % 10)
      .withOpenId(s"openid-${i % 17}")
      .withProfile(
        User.Profile(
          s"nickname-$i",
          s"avatar-url-$i",
          s"lang-${i % 5}",
          i % 3
        )
      )

    if (i % 17 == 0)
      u.withGeoLocation(User.GeoLocation())
    else u.withGeoLocation(
      User.GeoLocation(
        s"city-${i % 13}",
        s"province-${i % 7}",
        s"country-${i % 7}"
      )
    )
  }

  it should "put users successfully" in {
    for (u <- users) {
      val res = table.putUser(u.appId, u.openId, u.getProfile, u.getGeoLocation)
      assert(res.isSuccess)
    }
  }

  it should "get users successfully" in {
    for (i <- 0 until 10) {
      val res = table.queryUsers(i)
      assert(res.size == 10)
      for (u <- res) {
        assert(u.appId == i)
      }
    }
  }

  it should "query users with openids successfully" in {
    val openIds = for (i <- 0 until 17) yield s"openid-$i"
    for (i <- 0 until 10) {
      openIds.iterator.sliding(4, 2).foreach { subids =>
        val idset = subids.toSet
        val res = table.queryUsersWithOpenIds(i, idset)
        assert(res.size == users.count(u => u.appId == i && idset.contains(u.openId)))
        res.foreach { u =>
          assert(u.appId == i)
          assert(idset.contains(u.openId))
        }
      }
    }
  }
}