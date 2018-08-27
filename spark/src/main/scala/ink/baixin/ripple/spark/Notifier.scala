package ink.baixin.ripple.spark

import scala.util.Try
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http._
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.ssl.SslContext
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import spray.json._
import com.typesafe.scalalogging.Logger
import java.net.URI
import java.util.Date
import ink.baixin.ripple.core.models.Session

object Notifier {
  final case class Event(
                          app_id: Int,
                          open_id: String,
                          `type`: String,
                          subtype: String,
                          parameter: Long = 0,
                          extra_parameter: String = "",
                          dwell_time: Int = 0,
                          repeat_times: Int = 0,
                          timestamp: Date
                        )

  object JsonProtocol extends DefaultJsonProtocol {
    implicit object dateFormat extends JsonFormat[Date] {
      import java.util.TimeZone
      import java.text.SimpleDateFormat

      private val df = {
        val tz = TimeZone.getTimeZone("UTC")
        val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        format.setTimeZone(tz)
        format
      }

      def write(d: Date) = JsString(df.format(d))
      def read(value: JsValue) = value match {
        case JsString(ds) => df.parse(ds)
        case _ => deserializationError("ISO date string expected")
      }
    }

    implicit val eventFormat = jsonFormat9(Event)
  }
}

class Notifier(uri: URI, user: String, pass: String) {
  private val logger = Logger(this.getClass)

  class HttpClientHandler extends SimpleChannelInboundHandler[HttpObject] {
    override def channelRead0(ctx: ChannelHandlerContext, msg: HttpObject) {
      msg match {
        case resp: HttpResponse if resp.getStatus == HttpResponseStatus.OK =>
          logger.info(s"event=response_ok uri=$uri")
        case resp: HttpResponse =>
          logger.warn(s"event=response_error uri=$uri status=$resp")
        case _ =>
        // do nothing
      }
      ctx.close()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) {
      logger.error(s"event=reponse_handle_error uri=$uri error=$e")
      ctx.close
    }
  }

  class HttpClientInitializer(private val sslCtx: Option[SslContext]) extends ChannelInitializer[SocketChannel] {
    override def initChannel(ch: SocketChannel) {
      val p = ch.pipeline
      sslCtx.foreach { ctx => p.addLast(ctx.newHandler(ch.alloc())) }
      p.addLast(new HttpClientCodec())
      p.addLast(new HttpClientHandler())
    }
  }

  private val scheme = if (uri.getScheme == null) "http" else uri.getScheme.toLowerCase

  private val host = uri.getHost
  private val port = if (uri.getPort < 0) {
    scheme match {
      case "http" => 80
      case "https" => 443
      case _ => throw new UnsupportedOperationException()
    }
  } else uri.getPort

  private val basicAuthString = {
    import java.util.Base64
    s"Basic ${Base64.getEncoder.encodeToString(s"$user:$pass".getBytes)}"
  }


  private val bootstrap = {
    val sslCtx = if (scheme == "https") {
      Some(SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build())
    } else {
      None
    }
    val group = new NioEventLoopGroup()
    new Bootstrap()
      .group(group)
      .channel(classOf[NioSocketChannel])
      .handler(new HttpClientInitializer(sslCtx))
      .remoteAddress(host, port)
  }

  private def request(payload: String) = {
    val bytes = payload.getBytes
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri.getRawPath)
    req.headers.set(HttpHeaderNames.HOST, host)
    req.headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
    req.headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
    req.headers.set(HttpHeaderNames.AUTHORIZATION, basicAuthString)
    req.headers.set(HttpHeaderNames.CONTENT_LENGTH, bytes.size)
    req.content.clear.writeBytes(bytes)
    req
  }

  def send(appId: Int, openId: String, event: Session.Event) = Try {
    logger.info(s"event=notify_event app_id=$appId open_id=$openId event=$event")
    import Notifier.JsonProtocol._
    val param = try { event.parameter.toLong } catch { case _: Throwable => 0L }
    val eve = Notifier.Event(
      appId, openId, event.`type`, event.subType, param, event.extraParameter,
      event.dwellTime, event.repeatTimes, new Date(event.timestamp)
    )
    val ch = bootstrap.connect().sync.channel
    ch.writeAndFlush(request(eve.toJson.compactPrint))
  }
}