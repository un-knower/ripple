package ink.baixin.ripple.scheduler.services

import java.io.{InputStream, OutputStream}
import java.net.URI

import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.scheduler.HadoopConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

object HadoopService {
  private val logger = Logger(this.getClass)

  lazy val hadoopConf = {
    val config = new Configuration()
    System.setProperty("HADOOP_USER_NAME", HadoopConfig.user)
    config.set("fs.defaultFS", HadoopConfig.defaultFS)
    config.set("yarn.resourcemanager.address", HadoopConfig.yarnRNAddress)
    config
  }

  lazy val yarnConf = new YarnConfiguration(hadoopConf)
  lazy val defaultFS = FileSystem.get(
    URI.create(HadoopConfig.defaultFS),
    hadoopConf,
    HadoopConfig.user
  )
  lazy val stagingRoot = new Path(defaultFS.getHomeDirectory, ".schedulerStaging")

  case class YarnAppContext(config: YarnConfiguration) {
    lazy val client = {
      val clt = YarnClient.createYarnClient()
      clt.init(config)
      logger.info("service=hadoop event=start_yarn_client")
      clt.start()
      clt
    }
    lazy val app = client.createApplication()
    lazy val appId = app.getNewApplicationResponse.getApplicationId
    lazy val stagingDirectory = new Path(stagingRoot, appId.toString)

    def getAppReport = client.getApplicationReport(appId)

    def getAppState = {
      val state = getAppReport.getYarnApplicationState
      logger.info(s"service=hadoop event=get_app_state app_id=$appId state=$state")
      if (state == YarnApplicationState.FINISHED) {
        getAppReport.getFinalApplicationStatus match {
          case FinalApplicationStatus.SUCCEEDED => YarnApplicationState.FINISHED
          case FinalApplicationStatus.KILLED => YarnApplicationState.KILLED
          case _ => YarnApplicationState.FAILED
        }
      } else {
        state
      }
    }

    def isTerminated = {
      val state = getAppState
      (state == YarnApplicationState.FINISHED
        || state == YarnApplicationState.FAILED
        || state == YarnApplicationState.KILLED)
    }

    def execute(f: YarnAppContext => Unit): Option[YarnApplicationState] = {
      try {
        f(this)
        // return None here to bypass compiling
        // the real result will be that in finally clause
        return None
      } catch {
        case e: Throwable =>
          logger.error(s"event=hadoop_app_failed error=$e stack_trace=${e.getStackTrace}")
          None
      } finally {
        // return the final state of the application
        return exit
      }
    }

    def exit: Option[YarnApplicationState] = {
      try {
        // make sure the app has terminated
        if (!isTerminated) client.killApplication(appId)
        // cleanup filesystem
        logger.info(s"service=hadoop event=cleanup_staging_directory directory=$stagingDirectory")
        defaultFS.delete(stagingDirectory, true)
        // explicitly return final state here
        return Some(getAppState)
      } finally {
        // stop client on context exited
        logger.info("service=hadoop event=stop_yarn_client")
        client.stop()
      }
      None
    }

    def submitApp(
                   apptype: String,
                   appname: String,
                   command: Seq[String],
                   amResource: Resource,
                   appenvs: Map[String, String],
                   localResources: Map[String, LocalResource]
                 ) = {
      val containerContext = ContainerLaunchContext.newInstance(
        localResources.asJava,
        appenvs.asJava,
        command.asJava,
        null, null,
        Map(
          ApplicationAccessType.VIEW_APP -> "* *",
          ApplicationAccessType.MODIFY_APP -> "* *"
        ).asJava
      )

      val submissionContext = {
        val context = app.getApplicationSubmissionContext
        context.setApplicationType(apptype)
        context.setApplicationName(appname)
        context.setAMContainerSpec(containerContext)
        context.setResource(amResource)
        context
      }

      logger.info(s"service=hadoop event=submit_app type=$apptype name=$appname envs=$appenvs cmd=$command")
      logger.debug(s"service=hadoop event=print_resources resources=${localResources.map(ent => (ent._1, ent._2.getResource.toString))}")
      client.submitApplication(submissionContext)
    }

    def wait(duration: FiniteDuration) = {
      val expire = System.nanoTime() + duration.toNanos
      do {
        Thread.sleep(4000 + Random.nextInt(2000))
      } while (!isTerminated && (System.nanoTime() < expire))
    }
  }

  def launchYarnApp(runApp: YarnAppContext => Unit) = {
    YarnAppContext(yarnConf).execute(runApp)
  }

  def getHDFSFileStatus(path: Path) = defaultFS.getFileStatus(path)

  def copyHDFSFile(src: Path, dest: Path) =
    FileUtil.copy(defaultFS, src, defaultFS, dest, false, hadoopConf)

  def writeHDFSFile(path: Path, overwrite: Boolean = false, permission: String = "664", replication: Short = 0)
                   (f: OutputStream => Unit) = {
    val fperm = FsPermission.createImmutable(Integer.parseInt(permission, 8).toShort)
    val frep = if (replication <= 0) defaultFS.getDefaultReplication(path) else (3).toShort
    val blockSize = defaultFS.getDefaultBlockSize(path)
    val out = defaultFS.create(path, fperm, overwrite, (1 << 22), frep, blockSize, null)
    try {
      f(out)
    } finally {
      out.close()
    }
  }

  def readHDFSFile(path: Path)(f: InputStream => Unit) = {
    // open file for reading and limit buffer size to 4 mb
    val in = defaultFS.open(path, (1 << 22))
    try {
      f(in)
    } finally {
      in.close()
    }
  }
}