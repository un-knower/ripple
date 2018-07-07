package ink.baixin.ripple.producer

import com.typesafe.scalalogging.Logger
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel

object Producer {
  private val logger = Logger(this.getClass)

  private val region = System.getProperty("AWS_REGION", "cn-north-1")
  private val stream = System.getProperty("KINESIS_STREAM", "wmp-analytics")
  private val port = System.getProperty("LISTENING_PORT", "8080").toInt

  def main(args: Array[String]): Unit = {
    val writer = new KinesisEventWriter("cn-north-1", "wmp-analytics", 1000)
    val bossGroup = new NioEventLoopGroup(2)
    val workerGroup = new NioEventLoopGroup()

    try {
      val http = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new HttpServerInitializer(writer))

      val f = http.bind(port).sync.channel
      logger.info(s"event=start_server port=$port")
      f.closeFuture.sync
    } finally {
      bossGroup.shutdownGracefully
      workerGroup.shutdownGracefully
    }

  }
}