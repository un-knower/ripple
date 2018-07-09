package ink.baixin.ripple.spark

import scopt.OptionParser

case class DimeJobOption(
                          region: String = "cn-north-1",
                          stream: String = "wmp-analytics",
                          appName: String = "dime-job-production",
                          endPointURL: String = "kinesis.cn-north-1.amazonaws.com.cn",
                          checkpointDirectory: String = "hdfs:////user/spark/streaming/checkpoint/dime/",
                          checkpointInterval: Int = 15
                        )

object DimeJobOptionParser extends OptionParser[DimeJobOption]("dimeJob") {
  head("dimeJob", getClass.getPackage.getImplementationVersion)
  opt[String]('r', "region").action((value, option) => option.copy(region = value))
  opt[String]('s', "stream").action((value, option) => option.copy(stream = value))
  opt[String]('n', "appname").action((value, option) => option.copy(appName = value))
  opt[String]('u', "endpoint-url").action((value, option) => option.copy(endPointURL = value))
  opt[String]('u', "checkpoint-directory").action((value, option) => option.copy(checkpointDirectory = value))
  opt[Int]('i', "checkpoint-interval")
    .action((value, option) => option.copy(checkpointInterval = value))
    .validate(v => if (v > 0) success else failure("interval must be greater than 0"))
}