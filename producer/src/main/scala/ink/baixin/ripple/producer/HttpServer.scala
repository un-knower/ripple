package ink.baixin.ripple.producer

import com.typesafe.scalalogging.Logger
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.{DefaultFullHttpResponse, HttpRequest, HttpVersion}

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
    }
  }
}
