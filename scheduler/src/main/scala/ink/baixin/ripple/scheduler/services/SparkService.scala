package ink.baixin.ripple.scheduler.services

import java.util.Properties
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}
import com.typesafe.scalalogging.Logger
import ink.baixin.ripple.scheduler.SparkConfig
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import scala.concurrent.duration.FiniteDuration

object SparkService {
  private val logger = Logger(this.getClass)
  val AM_MEMROY = 1024
  val AM_CORES = 1
  val SPARK_APP_TYPE = "SPARK"
  val SPARK_CONF_DIR = "__spark_conf__"
  val SPARK_JARS_DIR = "__spark_libs__"
  val SPARK_APP_JAR = "__app__.jar"

  def runSparkApp(appJar: String, name: String, argm: Map[String, String], retries: Int): Option[YarnApplicationState] = {
    if (retries <= 0) {
      logger.info(s"service=spark event=retry_time_exceed name=$name")
      Some(YarnApplicationState.FAILED)
    } else {
      logger.info(s"service=spark event=run_app name=$name retries=$retries")
      executeSparkApp(appJar, name, argm) match {
        case Some(YarnApplicationState.FINISHED) => Some(YarnApplicationState.FINISHED)
        case _ => runSparkApp(appJar, name, argm, retries - 1)
      }
    }
  }

  private def executeSparkApp(appJar: String, name: String, argm: Map[String, String]): Option[YarnApplicationState] = {
    HadoopService.launchYarnApp { context =>
      // staging directory used while submitting applications
      val stagingDir = context.stagingDirectory
      // get local execute environment
      val envs = Map(
        "HADOOP_USER_NAME" -> "hadoop",
        "SPARK_USER" -> "hadoop",
        "SPARK_YARN_MODE" -> "true",
        "SPARK_YARN_STAGING_DIR" -> stagingDir.toString,
        Environment.CLASSPATH.name -> getClassPath
      )
      val args = argm.toSeq.flatMap(arg => Seq(s"--${arg._1}", arg._2))
      // get execute command
      val command = getCommand(name, args, stagingDir)
      val amResource = Resource.newInstance(AM_MEMROY + Math.max(AM_MEMROY / 4, 128), AM_CORES)
      // get local resource from hdfs
      val localResources = getLocalResource(appJar, stagingDir)

      context.submitApp(
        SPARK_APP_TYPE,
        name,
        command,
        amResource,
        envs,
        localResources
      )
      context.wait(FiniteDuration(60, "minuters"))
    }
  }

  def getClassPath = {
    val app = Seq(
      Environment.PWD.$$(),
      s"${Environment.PWD.$$()}/$SPARK_CONF_DIR",
      s"${Environment.PWD.$$()}/$SPARK_JARS_DIR/*"
    )
    val yarn = YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH
    val aws = Seq(
      "/usr/lib/hadoop-lzo/lib/*",
      "/usr/share/aws/emr/emrfs/conf",
      "/usr/share/aws/emr/emrfs/lib/*",
      "/usr/share/aws/emr/emrfs/auxlib/*",
      "/usr/share/aws/emr/lib/*",
      "/usr/share/aws/emr/ddb/lib/emr-ddb-hadoop.jar",
      "/usr/share/aws/emr/goodies/lib/emr-hadoop-goodies.jar",
      "/usr/share/aws/emr/kinesis/lib/emr-kinesis-hadoop.jar",
      "/usr/lib/spark/yarn/lib/datanucleus-api-jdo.jar",
      "/usr/lib/spark/yarn/lib/datanucleus-core.jar",
      "/usr/lib/spark/yarn/lib/datanucleus-rdbms.jar",
      "/usr/share/aws/emr/cloudwatch-sink/lib/",
      "/usr/share/aws/aws-java-sdk/*"
    )
    val mr = MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH.split(",")
    (app ++ yarn ++ aws ++ mr).mkString(ApplicationConstants.CLASS_PATH_SEPARATOR)
  }

  def escape(arg: String) = {
    val escaped = new StringBuilder("'")
    arg.foreach {
      case '$' => escaped.append("\\$")
      case '"' => escaped.append("\\\"")
      case '\'' => escaped.append("\\\'")
      case c => escaped.append(c)
    }
    escaped.append("'").toString()
  }

  def getCommand(name: String, arguments: Seq[String], stagingDir: Path): Seq[String] = {
    val appJarTarget = new Path(stagingDir, SPARK_APP_JAR)
    val javaCmd = Seq(
      s"${Environment.JAVA_HOME.$$()}/bin/java",
      "-server"
    )
    val javaOpts = Seq(
      "-XX:+UseConcMarkSweepGC",
      "-XX:MaxHeapFreeRatio=70",
      "-XX:+CMSClassUnloadingEnabled",
      "-XX:CMSInitiatingOccupancyFraction=70",
      "-XX:OnOutOfMemoryError='kill -9 %p'",
      s"-Xmx$AM_MEMROY",
      s"-Djava.io.tmpdir=${Environment.PWD.$$()}/tmp",
      s"-Dspark.yarn.app.container.log.dir=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}"
    )
    val cmdArgs = Seq(
      "org.apache.spark.deploy.yarn.ApplicationMaster",
      "--class", s"$name",
      "--jar", appJarTarget.toUri.toString,
      "--properties-file", s"${Environment.PWD.$$()}/$SPARK_CONF_DIR/__spark_conf__.properties"
    )
    val appArgs = arguments.flatMap(s => Seq("--arg", escape(s)))
    val logArgs = Seq(
      s"1> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout",
      s"2> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr"
    )
    javaCmd ++ javaOpts ++ cmdArgs ++ appArgs ++ logArgs
  }

  def getLocalResource(appJar: String, stagingDir: Path): Map[String, LocalResource] = {
    val appJarTarget = new Path(stagingDir, SPARK_APP_JAR)
    val libArchiveTarget = HadoopService.getHDFSFileStatus(new Path(SparkConfig.sparkLibsArchive)).getPath
    val confTarget = new Path(stagingDir, SPARK_CONF_DIR + ".zip")
    // Copy app jar to staging folder in case it will be overwritten while executing programs
    HadoopService.copyHDFSFile(new Path(appJar), appJarTarget)
    // get file status
    val appJarStatus = HadoopService.getHDFSFileStatus(appJarTarget)
    val libArchiveStatus = HadoopService.getHDFSFileStatus(libArchiveTarget)

    val cacheFileProperties = {
      val prop = new Properties()
      prop.setProperty(
        "spark.yarn.cache.filenames",
        Seq(
          s"${appJarTarget.toUri.toString}#$SPARK_APP_JAR",
          s"${libArchiveTarget.toUri.toString}#$SPARK_JARS_DIR"
        ).mkString(",")
      )
      prop.setProperty("spark.yarn.archive", libArchiveTarget.toUri.toString)
      prop.setProperty("spark.yarn.cache.types", Seq("FILE", "ARCHIVE").mkString(","))
      prop.setProperty("spark.yarn.cache.visibilities", Seq("PRIVATE", "PUBLIC").mkString(","))
      prop.setProperty(
        "spark.yarn.cache.timestamps",
        Seq(
          appJarStatus.getModificationTime.toString,
          libArchiveStatus.getModificationTime.toString
        ).mkString(",")
      )
      prop.setProperty(
        "spark.yarn.cache.sizes",
        Seq(
          appJarStatus.getLen.toString,
          libArchiveStatus.getLen.toString
        ).mkString(",")
      )
      prop.setProperty("spark.yarn.cache.confArchive", confTarget.toUri.toString)

      prop
    }

    // copy spark conf file from spark directory to yarn directory
    HadoopService.writeHDFSFile(confTarget) { out =>
      val zipOut = new ZipOutputStream(out)
      val buffer = new Array[Byte](1 << 10) // set buffer size to 1 mb

      HadoopService.readHDFSFile(new Path("hdfs:///apps/spark/spark-conf.zip")) { in =>
        val zipIn = new ZipInputStream(in)
        // get an uncompressed file from the zip file
        var entry = zipIn.getNextEntry
        while (entry != null) {
          zipOut.putNextEntry(new ZipEntry(entry.getName))
          while (zipIn.available() > 0) {
            val n = zipIn.read(buffer)
            if (n > 0) zipOut.write(buffer, 0, n)
          }

          if (entry.getName == "__spark_conf__.properties") {
            zipOut.write("\n\n".getBytes())
            cacheFileProperties.store(zipOut, "Cache file configurations")
          }

          zipOut.closeEntry()
          entry = zipIn.getNextEntry
        }
        zipIn.close()
      }

      zipOut.flush()
      zipOut.close()
    }
    val confStatus = HadoopService.getHDFSFileStatus(confTarget)

    Map(
      SPARK_APP_JAR -> LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(appJarTarget),
        LocalResourceType.FILE,
        LocalResourceVisibility.PUBLIC,
        appJarStatus.getLen,
        appJarStatus.getModificationTime
      ),
      SPARK_JARS_DIR -> LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(libArchiveTarget),
        LocalResourceType.ARCHIVE,
        LocalResourceVisibility.PRIVATE,
        libArchiveStatus.getLen,
        libArchiveStatus.getModificationTime
      ),
      SPARK_CONF_DIR -> LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(confTarget),
        LocalResourceType.ARCHIVE,
        LocalResourceVisibility.PRIVATE,
        confStatus.getLen,
        confStatus.getModificationTime
      )
    )
  }
}
