package top.doe.netty.demo1

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}


class ClientHandler1 extends ChannelInboundHandlerAdapter {

  /**
   * 一旦跟服务端建立上连接，channelActive方法将被调用
   */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("ClientHandler的channelActive方法被调用！【已经跟服务端连接上了】")
  }

  /**
   * 服务端返回消息后，channelRead方法被调用，该方法用于接送服务端返回的消息
   */
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    println("ClientHandler的channelRead方法被调用！")
  }

}
