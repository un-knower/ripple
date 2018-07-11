package ink.baixin.ripple.query.server

import java.sql.DriverManager
import org.apache.commons.lang3.StringEscapeUtils
import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._
import com.typesafe.scalalogging.Logger
import scala.concurrent.Promise

object RippleQueryServer {
  final case class Query(sql: String, project: String)
  final case class ColumnMeta(
                               label: String, name: String, columnType: Int, columnTypeName: String, isNullable: Int
                             )
  final case class Result(
                           cube: String, isException: Boolean, exceptionMessage: String, duration: Long,
                           columnMetas: Seq[ColumnMeta], results: Seq[Seq[String]]
                         )

  trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
    implicit val queryFormat = jsonFormat2(Query)
    implicit val columnMetaFormat = jsonFormat5(ColumnMeta)
    implicit val resultFormat = jsonFormat6(Result)
  }
}

class RippleQueryServer(project: String, listenPort: Int) extends Directives with RippleQueryServer.JsonSupport {
  private val logger = Logger(this.getClass)

  import RippleQueryServer._

  implicit val system = ActorSystem(project)
  implicit val materializer = akka.stream.ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher

  private lazy val dbConnection = {
    val properties = {
      val p = new java.util.Properties()
      p.setProperty("lex", "MYSQL_ANSI")
      p.setProperty("schema", project)
      p.setProperty("quoting", "DOUBLE_QUOTE")
      p.setProperty("schemaFactory", "ink.baixin.ripple.query.schema.RippleSchemaFactory")
      p
    }
    val conn = DriverManager.getConnection("jdbc:calcite:", properties)
    logger.info(s"evnet=get_connection path=${conn.getMetaData.getURL}")
    conn
  }

  private val route =
    get {
      complete(StatusCodes.OK) // return 200 at root path for health check
    } ~
      path("ripple" / "api" / "query") {
        post {
          entity(as[Query]) {
            query =>
              val res = executeQuery(query)
              if (res.isException) complete(StatusCodes.BadRequest -> res)
              else complete(StatusCodes.OK -> res)
          }
        }
      }

  private def executeQuery(query: Query) = {
    val escapedSql = StringEscapeUtils.escapeJava(query.sql)
    logger.info(s"event=execute_query project=${query.project} sql=${escapedSql}")
    val start = System.nanoTime
    var pstmt: java.sql.PreparedStatement = null
    var result: java.sql.ResultSet = null
    try {
      pstmt = dbConnection.prepareStatement(query.sql)
      result = pstmt.executeQuery
      val meta = pstmt.getMetaData
      val cols = meta.getColumnCount
      val colMeta = for (i <- 1 to cols) yield {
        ColumnMeta(
          meta.getColumnLabel(i),
          meta.getColumnName(i),
          meta.getColumnType(i),
          meta.getColumnTypeName(i),
          meta.isNullable(i)
        )
      }
      val rows = scala.collection.mutable.ListBuffer[Seq[String]]()
      while (result.next()) {
        val row = (for (i <- 1 to cols) yield {
          val v = result.getObject(i)
          if (v == null) ""
          else v.toString
        })
        rows.append(row.toSeq)
      }
      val timeCost = (System.nanoTime - start) / 1000000
      logger.info(s"event=query_finished project=${query.project} sql='${escapedSql}' time_cost=${timeCost} rows=${rows.size}")
      Result(project, false, "", timeCost, colMeta.toSeq, rows.toSeq)
    } catch {
      case e: Throwable =>
        val timeCost = (System.nanoTime - start) / 1000000
        logger.error(s"event=query_execute_error project=${query.project} sql='${escapedSql}' time_cost=${timeCost} error=$e")
        Result(project, true, e.toString, timeCost, Seq(), Seq())
    } finally {
      if (result != null) result.close
      if (pstmt != null) pstmt.close
    }
  }

  def start() {
    val bindingFuture = Http().bindAndHandle(route, "0.0.0.0", 8080)
    val promise = Promise[Done]()
    sys.addShutdownHook {
      promise.trySuccess(Done)
    }
    // Unbind from the port and shut down when done
    for(_ <- bindingFuture; _ <- promise.future) {
      logger.info("event=server_shutting_down")
      dbConnection.close
      bindingFuture
        .flatMap(_.unbind())
        .onComplete(_ => system.terminate())
    }
  }
}