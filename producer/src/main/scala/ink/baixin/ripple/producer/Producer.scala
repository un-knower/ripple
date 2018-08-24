package ink.baixin.ripple.producer

import com.typesafe.scalalogging.Logger
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel

object Producer {
  private val logger = Logger(this.getClass)
  private val region = System.getProperty("AWS_REGION", "cn-north-1")
  private val port = System.getProperty("LISTENING_PORT", "8080").toInt

  def main(args: Array[String]): Unit = {
    val writer = new KinesisEventWriter(region, 1000)

    // bossGroup is for client connection, workerGroup is for handling
    // if there are multiple `ServerBootstrap`, we should set more than one thread
    val bossGroup = new NioEventLoopGroup(1)
    val workerGroup = new NioEventLoopGroup()
    try {
      // use ServerBootstrap to initialize and start
      val http = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new HttpServerInitializer(writer))

      val f = http.bind(port).sync().channel()
      logger.info(s"event=start_server port=$port")
      f.closeFuture().sync()
    } finally {
      bossGroup.shutdownGracefully()
      workerGroup.shutdownGracefully()
    }
  }
}