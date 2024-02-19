package top.doe.netty.demo1

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

class ServerHandler1 extends ChannelInboundHandlerAdapter {

  /**
   * 有客户端与服务端建立连接后调用
   */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("ServerHandler的channelActive方法被调用【一个客户端连接上】")
  }

  /**
   * 有客户端与服务端断开连接后调用
   */
  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    println("ServerHandler的channelInactive方法被调用【一个客户端与服务端端口连接了】")
  }

  /**
   * 接受客户端发送来的消息
   */
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    println("ServerHandler的channelRead方法被调用【收到客户端发送的消息了】")
  }

}
