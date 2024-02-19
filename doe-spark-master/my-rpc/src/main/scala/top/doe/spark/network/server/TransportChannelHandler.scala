package top.doe.spark.network.server

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import top.doe.spark.network.client.{TransportClient, TransportResponseHandler}
import top.doe.spark.network.protocol.{Message, RequestMessage, ResponseMessage}

//是Netty处理请求和响应的处理器
class TransportChannelHandler(
  val client: TransportClient,
  val responseHandler: TransportResponseHandler,
  val requestHandler: TransportRequestHandler
) extends SimpleChannelInboundHandler[Message]{


  //建立连接后调用
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    requestHandler.channelActive()
    responseHandler.channelActive()
    super.channelActive(ctx)
  }

  //读取接收到的消息
  override def channelRead0(ctx: ChannelHandlerContext, message: Message): Unit = {
    message match {
      case requestMessage: RequestMessage => {
        requestHandler.handle(requestMessage)
      }
      case responseMessage: ResponseMessage => {
        //处理响应
        responseHandler.handle(responseMessage)
      }
      case _ =>
        ctx.fireChannelRead(message)
    }
  }

  def getClient: TransportClient = client
}
