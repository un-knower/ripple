package ink.baixin.ripple.query

import com.typesafe.scalalogging.Logger
import sqlline.SqlLine
import ink.baixin.ripple.query.server.RippleQueryServer

object RippleQuery {
  private val logger = Logger(this.getClass)

  def main(args: Array[String]) {
    val config = RippleQueryOption.parse(args, RippleQueryOption())
    config match {
      case Some(RippleQueryOption("server", project, port)) =>
        // run server
        logger.info(s"running API server...")
        new RippleQueryServer(project, port).start()
      case Some(RippleQueryOption("shell", project, port)) =>
        // run shell
        logger.info(s"running SQL shell...")
        SqlLine.main(Array())
      case _ => // do nothing
    }
  }
}
