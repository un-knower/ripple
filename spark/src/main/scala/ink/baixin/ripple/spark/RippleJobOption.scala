package ink.baixin.ripple.spark

import scopt.OptionParser

case class RippleJobOption(
                          region: String = "cn-north-1",
                          stream: String = "wmp-analytics",
                          appName: String = "production",
                          taskCategory: String = "streaming",
                          namespaces: Set[String] = Set("", "/w", "/wpre"),
                          endPointURL: String = "kinesis.cn-north-1.amazonaws.com.cn",
                          checkpointDirectory: String = "s3://ripple-checkpoint/ripple-production/",
                          checkpointInterval: Int = 10
                        )

object RippleJobOptionParser extends OptionParser[RippleJobOption]("rippleJob") {
  head(
    "rippleJob",
    getClass.getPackage.getImplementationVersion
  )

  private val taskNames = Set(
    "batch",
    "streaming"
  )

  opt[String]('r', "region").action((value, option) => option.copy(region = value))
  opt[String]('s', "stream").action((value, option) => option.copy(stream = value))
  opt[String]('n', "appname").action((value, option) => option.copy(appName = value))
  opt[String]('t', "taskcategory").action((value, option) => option.copy(taskCategory = value))
    .validate(n => if (taskNames.contains(n)) success else failure(s"task name must be one of $taskNames"))
  opt[Seq[String]]("namespaces").abbr("ns").action((value, option) => option.copy(namespaces = value.toSet))
  opt[String]('u', "endpoint-url").action((value, option) => option.copy(endPointURL = value))
  opt[String]('d', "checkpoint-directory").action((value, option) => option.copy(checkpointDirectory = value))
  opt[Int]('i', "checkpoint-interval")
    .action((value, option) => option.copy(checkpointInterval = value))
    .validate(v => if(v > 0) success else failure("interval must be greater than 0"))
}