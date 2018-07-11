package ink.baixin.ripple.core.documents

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec
import com.typesafe.scalalogging.Logger
import scala.util.{ Try, Success, Failure }
import ink.baixin.ripple.core.models._

class UserTable(private val table: Table) {
  private val logger = Logger(this.getClass)

  private def safeQuery(spec: QuerySpec) =
    Try(table.query(spec).iterator()) match {
      case Success(iter) =>
        new Iterator[Item]() {
          override def hasNext() = iter.hasNext()
          override def next() = iter.next()
        }
      case Failure(e) =>
        Seq[Item]().iterator
    }

  private def safeScan(spec: ScanSpec) =
    Try(table.scan(spec).iterator()) match {
      case Success(iter) =>
        new Iterator[Item]() {
          override def hasNext() = iter.hasNext()
          override def next() = iter.next()
        }
      case Failure(e) =>
        Seq[Item]().iterator
    }

  private def unpackUser(pk:Int, openId: String, attrs: Seq[String], item: Item) =
    attrs.foldLeft(
      User().withAppId(pk).withOpenId(openId)
    ) {
      case (u, "prf") if item.hasAttribute("prf") =>
        u.withProfile(User.Profile.parseFrom(item.getBinary("prf")))
      case (u, "geo") if item.hasAttribute("geo") =>
        u.withGeoLocation(User.GeoLocation.parseFrom(item.getBinary("geo")))
      case (u, _) => u
    }

  private def packField(field: Any) = {
    field match {
      case Some(f: User.Profile) => f.toByteArray
      case Some(f: User.GeoLocation) => f.toByteArray
      case _ => new Array[Byte](0)
    }
  }

  val allAttributes = Seq("prf", "geo")

  def getUser(appId: Int, openId: String, attrs: Seq[String] = allAttributes) = Try {
    logger.debug(s"event=get_user pk=$appId openid=$openId")
    val spec = new GetItemSpec()
      .withPrimaryKey("aid", appId, "oid", openId)
      .withAttributesToGet(attrs: _*)
    val res = table.getItem(spec)
    unpackUser(appId, openId, attrs, res)
  }

  def putUser(user: User) = Try {
    logger.debug(s"event=put_user pk=${user.appId} openid=${user.openId}")
    val keyItem = (new Item())
      .withPrimaryKey("aid", user.appId, "oid", user.openId)

    val item = Seq(
      ("prf", packField(user.profile)),
      ("geo", packField(user.geoLocation))
    ).foldLeft(keyItem){
      case (it, (attr, value)) if !value.isEmpty => it.withBinary(attr, value)
      case (it, _)  => it
    }

    table.putItem(item)
  }

  def queryUsers(appId: Int, attrs: Seq[String] = allAttributes) = {
    logger.debug(s"event=query_users pk=$appId attrs=$attrs")
    val spec = new QuerySpec()
      .withHashKey("aid", appId)
      .withAttributesToGet((attrs :+ "oid"): _*)

    safeQuery(spec).map {
      (item) =>
        unpackUser(appId, item.getString("oid"), attrs, item)
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
        unpackUser(appId, item.getString("oid"), attrs, item)
    }
  }

  def scan(attrs: Seq[String] = allAttributes) = {
    logger.debug(s"event=scan_users attrs=$attrs")
    val spec = new ScanSpec()
      .withAttributesToGet((Seq("aid", "oid") ++ attrs): _*)

    safeScan(spec).map {
      (item) =>
        unpackUser(item.getInt("aid"), item.getString("oid"), attrs, item)
    }
  }
}