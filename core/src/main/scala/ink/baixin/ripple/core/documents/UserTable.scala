package ink.baixin.ripple.core.documents

import com.amazonaws.services.dynamodbv2.document.{Item, QueryFilter, RangeKeyCondition, Table}
import com.amazonaws.services.dynamodbv2.document.spec.{GetItemSpec, QuerySpec}
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.models._

import scala.util.{Failure, Success, Try}

class UserTable(private val table: Table) {
  private val logger = Logger(this.getClass)
  val allAttributes = Seq("prf", "geo")

  private def safeQuery(spec: QuerySpec) =
    Try(table.query(spec).iterator) match {
      case Success(iter) =>
        new Iterator[Item] {
          override def hasNext: Boolean = iter.hasNext

          override def next(): Item = iter.next
        }
      case Failure(e) => Seq[Item]().iterator
    }

  private def buildUser(pk: Int, openId: String, attrs: Seq[String], item: Item) =
    attrs.foldLeft(
      User().withAppId(pk).withOpenId(openId)
    ) {
      case (u, "prf") if item.hasAttribute("prf") =>
        u.withProfile(User.Profile.parseFrom(item.getBinary("prf")))
      case (u, "geo") if item.hasAttribute("geo") =>
        u.withGeoLocation(User.GeoLocation.parseFrom(item.getBinary("geo")))
      case (u, _) => u
    }

  def getUser(appId: Int, openId: String, attrs: Seq[String] = allAttributes) = Try {
    logger.debug(s"event=get_user pk=$appId openid=$openId")
    val spec = new GetItemSpec()
      .withPrimaryKey("aid", appId, "oid", openId)
      .withAttributesToGet(attrs: _*)
    val res = table.getItem(spec)
    buildUser(appId, openId, attrs, res)
  }

  def putUser(appId: Int,
              openId: String,
              profile: User.Profile,
              geoLocation: User.GeoLocation) = Try {
    logger.debug(s"event=put_user pk=$appId openid=$openId")
    val keyItem = new Item()
      .withPrimaryKey("aid", appId, "oid", openId)

    val item = Seq(
      ("prf", profile.toByteArray),
      ("geo", geoLocation.toByteArray)
    ).foldLeft(keyItem) {
      case (it, (attr, value)) if !value.isEmpty => it.withBinary(attr, value)
      case (it, _) => it
    }
    table.putItem(item)
  }

  def queryUsers(appId: Int, attrs: Seq[String] = allAttributes) = {
    logger.debug(s"event=query_users pk=$appId attrs=$attrs")
    val spec = new QuerySpec()
      .withHashKey("aid", appId)
      .withAttributesToGet((attrs :+ "oid"): _*)
    safeQuery(spec).map { item =>
      buildUser(appId, item.getString("oid"), attrs, item)
    }
  }

  def queryUsersWithOpenIds(appId: Int, openIds: Set[String], attrs: Seq[String] = allAttributes) = {
    logger.debug(s"event=query_users_with_openids pk=$appId openids=$openIds attrs=$attrs")
    val spec = new QuerySpec()
      .withHashKey("aid", appId)
      .withRangeKeyCondition(new RangeKeyCondition("oid").between(openIds.min, openIds.max))
      .withAttributesToGet((attrs :+ "oid"): _*)

    safeQuery(spec).collect {
      case item if openIds.contains(item.getString("oid")) =>
        buildUser(appId, item.getString("oid"), attrs, item)
    }
  }
}