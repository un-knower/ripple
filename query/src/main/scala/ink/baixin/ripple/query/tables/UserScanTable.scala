package ink.baixin.ripple.query.tables

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.calcite.DataContext
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.linq4j.AbstractEnumerable
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.core.StateProvider
import ink.baixin.ripple.core.models.User
import ink.baixin.ripple.query.utils.RippleEnumerator

class UserScanTable(private val sp: StateProvider) extends RippleTable {
  private val logger = Logger(this.getClass)

  private lazy val table = sp.resolver.getUserTable.get

  override val fields = Seq(
    ("app_id", SqlTypeName.INTEGER),
    ("open_id", SqlTypeName.VARCHAR),
    ("nickname", SqlTypeName.VARCHAR),
    ("avatar_url", SqlTypeName.VARCHAR),
    ("language", SqlTypeName.VARCHAR),
    ("gender", SqlTypeName.INTEGER),
    ("city", SqlTypeName.VARCHAR),
    ("province", SqlTypeName.VARCHAR),
    ("country", SqlTypeName.VARCHAR)
  )

  override val nullableFields =
    Set("nickname", "avatar_url", "language", "city", "province", "country")

  override val fieldGroups = Map(
    "prf" -> Set("nickname", "avatar_url", "language", "gender"),
    "geo" -> Set("city", "province", "country")
  )

  private def getFieldValue(user: User, field: String): AnyRef =
    field match {
      case "app_id" =>
        user.appId.asInstanceOf[java.lang.Integer]
      case "open_id" =>
        user.openId
      case "nickname" =>
        user.profile match {
          case Some(profile) => profile.nickname
          case None => null
        }
      case "avatar_url" =>
        user.profile match {
          case Some(profile) => profile.avatarUrl
          case None => null
        }
      case "language" =>
        user.profile match {
          case Some(profile) => profile.language
          case None => null
        }
      case "gender" =>
        user.profile match {
          case Some(profile) => profile.gender.asInstanceOf[java.lang.Integer]
          case None => java.lang.Integer.valueOf(0) // unknown
        }
      case "city" =>
        user.geoLocation match {
          case Some(geo) => geo.city
          case None => null
        }
      case "province" =>
        user.geoLocation match {
          case Some(geo) => geo.province
          case None => null
        }
      case "country" =>
        user.geoLocation match {
          case Some(geo) => geo.country
          case None => null
        }
    }

  private def perfermScan(ctx: DataContext, fields: Seq[String])(func: => Iterator[User]) = {
    val cancelFlag: AtomicBoolean = DataContext.Variable.CANCEL_FLAG.get(ctx)
    new AbstractEnumerable[AnyRef]() {
      override def enumerator(): Enumerator[AnyRef] =
        RippleEnumerator[User](cancelFlag, func) {
          (user) =>
            fields.map((f) => getFieldValue(user, f)).toArray
        }
    }
  }

  override def scan(ctx: DataContext, predicate: RippleTable.Predicate) = {
    val fields = predicate.fields match {
      case Some(f) => f
      case None => fieldKeys
    }
    val attrs = getAttrs(fields)

    predicate match {
      case RippleTable.Predicate(Some(appId), _, Some(openIds), _, _) =>
        logger.info(s"event=query_users app_id=$appId open_ids=$openIds attrs=$attrs")
        perfermScan(ctx, fields)(table.queryUsersWithOpenIds(appId, openIds, attrs))
      case RippleTable.Predicate(Some(appId), _, _, _, _) =>
        logger.info(s"event=query_users app_id=$appId attrs=$attrs")
        perfermScan(ctx, fields)(table.queryUsers(appId, attrs))
      case _ =>
        logger.warn(s"event=naive_scan_users attrs=$attrs")
        perfermScan(ctx, fields)(table.scan(attrs))
    }
  }
}