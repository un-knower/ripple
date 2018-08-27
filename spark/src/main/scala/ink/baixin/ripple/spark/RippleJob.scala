package ink.baixin.ripple.spark

import com.typesafe.scalalogging.Logger
import com.typesafe.config.ConfigFactory
import ink.baixin.ripple.core.StateProvider

object RippleJob {
  private val logger = Logger(this.getClass)

  def initState(appName: String) = {
    logger.info(s"event=init_state_tasks")
    val config = ConfigFactory.load.getConfig("ripple")
    val sp = new StateProvider(appName, config)
    sp.ensureState

    logger.info(s"event=ensure_tables")
    // get resource in ahead of time to materialize them
    sp.resolver.getFactTable
    sp.resolver.getUserTable
    sp.resolver.getCountTable
    sp.resolver.getAggregationTable
    sp
  }

  def main(args: Array[String]) {
    // use `scopt` to parse command line options
    // see `https://github.com/scopt/scopt` for more detail
    val option = RippleJobOptionParser.parse(args, RippleJobOption()).get
    val sp = initState(option.appName)
    option.taskCategory match {
      case "batch" => RippleBatchJob.run(option, sp)
      case "streaming" => RippleStreamingJob.run(option)
    }
  }
}