package ink.baixin.ripple.producer

import com.typesafe.scalalogging.Logger
import io.netty.buffer.Unpooled
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.handler.codec.http._
import io.netty.handler.timeout.ReadTimeoutHandler

class HttpServerHandler(private val writer: EventWriter)
  extends SimpleChannelInboundHandler[Object] {
  private val logger = Logger(this.getClass)

  private def forwardMessage(req: HttpRequest) {
    if (req.decoderResult.isSuccess) {
      val uri = req.uri
      logger.debug(s"event=forward_message url=$uri")
      writer.putEvent(System.currentTimeMillis, uri)
    } else {
      logger.error(s"event=request_decode_failure result=${req.decoderResult}")
    }
  }

  override def channelReadComplete(ctx: ChannelHandlerContext) {
    ctx.flush
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: Object) {
    if (msg.isInstanceOf[HttpRequest]) {
      logger.debug("event=flush_and_finish_requests")
      val response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT, Unpooled.EMPTY_BUFFER)
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
      forwardMessage(msg.asInstanceOf[HttpRequest])
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, error: Throwable) {
    logger.error(s"event=http_handle_error error=$error")
    ctx.close
  }

}

class HttpServerInitializer(private val writer: EventWriter) extends ChannelInitializer[SocketChannel] {
  private val logger = Logger(this.getClass)

  override def initChannel(ch: SocketChannel) {
    logger.debug("event=initialize_http_server_channel")
    ch.pipeline.addLast(
      new ReadTimeoutHandler(1),
      new HttpRequestDecoder(8192, 8192, 8192),
      new HttpResponseEncoder(),
      new HttpServerHandler(writer)
    )
  }
}
