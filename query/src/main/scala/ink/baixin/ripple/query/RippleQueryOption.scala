package ink.baixin.ripple.query

import scopt.OptionParser

case class RippleQueryOption(
                            command: String = "server",
                            projectName: String = "production",
                            listenPort: Int = 8080
                          )

object RippleQueryOption extends OptionParser[RippleQueryOption]("rippleQuery") {
  head(
    "rippleQuery",
    getClass.getPackage.getImplementationVersion
  )

  opt[String]('n', "project-name").action((v, c) => c.copy(projectName = v))

  cmd("server").action((_, c) => c.copy(command = "server"))
    .text("run ripple query's API server")
    .children(opt[Int]('p', "port").action((v, c) => c.copy(listenPort = v)))

  cmd("shell").action((_, c) => c.copy(command = "shell"))
    .text("run sqlline locally for testing")
}