package ink.baixin.ripple.spark

import java.net.URI

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.http._
import io.netty.handler.ssl.{SslContext, SslContextBuilder}
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import spray.json.DefaultJsonProtocol._
import spray.json._

object StreamingMonitor {
  final case class Message(action: String, service_name: String, status_code: String, message: String)
  implicit val messageFormat = jsonFormat4(Message)
}


class StreamingMonitor(appName: String) extends StreamingListener {
  import StreamingMonitor._

  private val logger = Logger(this.getClass)
  private val serviceName = s"ripple-streaming-$appName"
  val url = ConfigFactory.load().getConfig("ripple.pandora-url")
  private val uri = URI.create(url)
  private val port = 
      if (uri.getPort > 0) uri.getPort
      else if (uri.getScheme == "https") 443
      else 80

  class HttpClientHandler extends SimpleChannelInboundHandler[HttpObject] {
    override def channelRead0(ctx: ChannelHandlerContext, msg: HttpObject) {
      msg match {
        case resp: HttpResponse if resp.getStatus == HttpResponseStatus.OK =>
          logger.debug(s"event=monitor_send_message_ok uri=$uri status=$resp")
        case resp: HttpResponse =>
          logger.error(s"event=monitor_send_message_error uri=$uri status=$resp")
        case _ => 
          // do nothing
      }
      ctx.close()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
      logger.error(s"event=monitor_send_message_error uri=$uri error=$e")
      ctx.close
    }
  }

  class HttpClientInitializer(private val sslCtx: Option[SslContext]) extends ChannelInitializer[SocketChannel] {
    override def initChannel(ch: SocketChannel) {
      val p = ch.pipeline
      sslCtx.foreach { ctx => p.addLast(ctx.newHandler(ch.alloc(), uri.getHost, port)) }
      p.addLast(new HttpClientCodec())
      p.addLast(new HttpClientHandler())
    }
  }

  private val bootstrap = {
    val sslCtx = if (uri.getScheme == "https") {
      Some(SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build())
    } else {
      None
    }
    val group = new NioEventLoopGroup()
    new Bootstrap()
      .group(group)
      .channel(classOf[NioSocketChannel])
      .handler(new HttpClientInitializer(sslCtx))
      .remoteAddress(uri.getHost, port)
  }

  def request(payload: String) {
    val ch = bootstrap.connect().sync.channel
    val bytes = payload.getBytes
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri.getRawPath)
    req.headers.set(HttpHeaderNames.HOST, uri.getHost)
    req.headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
    req.headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
    req.headers.set(HttpHeaderNames.CONTENT_LENGTH, bytes.size)
    req.content.clear.writeBytes(bytes)
    ch.writeAndFlush(req)
  }

  override def onBatchCompleted(event: StreamingListenerBatchCompleted) {
    val info = event.batchInfo
    val hb = Message(
      "heartbeat",
      serviceName,
      "200",
      s"event=batch_completed num_records=${info.numRecords} schedule=${info.schedulingDelay} total=${info.totalDelay}"
    )
    request(hb.toJson.compactPrint)
  }
}