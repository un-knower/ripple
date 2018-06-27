package ink.baixin.ripple.core.documents

import com.amazonaws.services.dynamodbv2.document.{Item, QueryFilter, RangeKeyCondition, Table}
import com.amazonaws.services.dynamodbv2.document.spec.{GetItemSpec, QuerySpec}
import ink.baixin.ripple.core.models._

import scala.util.{Failure, Success, Try}

class UserTable(private val table: Table) {
  private def getUserKey(appId: Int, openId: String) = User.Key(appId, openId)

  private def safeQuery(spec: QuerySpec) =
    Try(table.query(spec).iterator) match {
      case Success(iter) =>
        new Iterator[Item] {
          override def hasNext: Boolean = iter.hasNext

          override def next(): Item = iter.next
        }
      case Failure(e) => Seq[Item]().iterator
    }

  def getUser(appId: Int, openId: String) = Try {
    val spec = new GetItemSpec()
      .withPrimaryKey("aid", appId, "oid", openId)
      .withAttributesToGet("prf", "geo")
    val res = table.getItem(spec)
    User(
      Some(User.Key(appId, openId)),
      Some(User.Profile.parseFrom(res.getBinary("prf"))),
      if (res.hasAttribute("geo")) Some(User.GeoLocation.parseFrom(res.getBinary("geo"))) else None
    )
  }

  def putUser(appId: Int,
              openId: String,
              profile: User.Profile,
              geo: Option[User.GeoLocation]) = {
    val item = new Item()
      .withPrimaryKey("aid", appId, "oid", openId)
      .withBinary("prf", profile.toByteArray)

    geo match {
      case Some(g) => table.putItem(item.withBinary("geo", g.toByteArray))
      case None => table.putItem(item)
    }
  }

  def queryUsers(appId: Int) = {
    val spec = new QuerySpec()
      .withHashKey("aid", appId)
      .withAttributesToGet("oid", "prf", "geo")
    safeQuery(spec).map { item =>
      User(
        Some(User.Key(appId, item.getString("oid"))),
        Some(User.Profile.parseFrom(item.getBinary("prf"))),
        if (item.hasAttribute("geo")) Some(User.GeoLocation.parseFrom(item.getBinary("geo"))) else None
      )
    }
  }

  def queryUsers(appId: Int, openIds: Set[String]) = {
    openIds.toSeq.sorted.iterator.sliding(100, 100).map { oids =>
      val spec = new QuerySpec()
        .withHashKey("aid", appId)
        .withRangeKeyCondition(new RangeKeyCondition("oid").between(oids.head, oids.last))
        .withQueryFilters(new QueryFilter("oid").in(oids: _*))
        .withAttributesToGet("oids", "prf", "geo")

      safeQuery(spec).map { item =>
        User(
          Some(User.Key(appId, item.getString("oid"))),
          Some(User.Profile.parseFrom(item.getBinary("prf"))),
          if (item.hasAttribute("geo")) Some(User.GeoLocation.parseFrom(item.getBinary("geo"))) else None
        )
      }
    }
  }

  def queryUserProfiles(appId: Int) = {
    val spec = new QuerySpec()
      .withHashKey("aid", appId)
      .withAttributesToGet("oid", "prf")

    safeQuery(spec).map { item =>
      (User.Key(appId, item.getString("oid")), User.Profile.parseFrom(item.getBinary("prf")))
    }
  }

  def queryUserProfiles(appId: Int, openIds: Set[String]) = {
    openIds.toSeq.sorted.iterator.sliding(100, 100).map { oids =>
      val spec = new QuerySpec()
        .withHashKey("aid", appId)
        .withRangeKeyCondition(new RangeKeyCondition("oid").between(oids.head, oids.last))
        .withQueryFilters(new QueryFilter("oid").in(oids: _*))
        .withAttributesToGet("oids", "prf")

      safeQuery(spec).map { item =>
        (User.Key(appId, item.getString("oid")), User.Profile.parseFrom(item.getBinary("prf")))
      }
    }
  }

  def queryUserGeoLocation(appId: Int) = {
    val spec = new QuerySpec()
      .withHashKey("aid", appId)
      .withAttributesToGet("oid", "geo")

    safeQuery(spec).map { item =>
      if (item.hasAttribute("geo"))
        Some(User.Key(appId, item.getString("oid")), User.GeoLocation.parseFrom(item.getBinary("geo")))
      else
        None
    }

    def queryUserGeoLocation(appId: Int, openIds: Set[String]) = {
      openIds.toSeq.sorted.iterator.sliding(100, 100).map { oids =>
        val spec = new QuerySpec()
          .withHashKey("aid", appId)
          .withRangeKeyCondition(new RangeKeyCondition("oid").between(oids.head, oids.last))
          .withQueryFilters(new QueryFilter("oid").in(oids: _*))
          .withAttributesToGet("oids", "geo")

        safeQuery(spec).map { item =>
          if (item.hasAttribute("geo"))
            Some(User.Key(appId, item.getString("oid")), User.GeoLocation.parseFrom(item.getBinary("geo")))
          else
            None
        }
      }
    }
  }
}