package top.doe.netty.demo2

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}


class ClientHandler2 extends ChannelInboundHandlerAdapter {

  /**
   * 一旦跟服务端建立上连接channelActive将被调用
   */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("ClientHandler的channelActive方法被调用！【已经跟服务端连接上了】")
    //向服务端发送消息
    val msg = "hello"
    ctx.writeAndFlush(Unpooled.copiedBuffer(msg.getBytes("UTF-8")))
  }

  /**
   * 接送服务端返回的消息
   */
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    val byteBuf = msg.asInstanceOf[ByteBuf]
    val bytes = new Array[Byte](byteBuf.readableBytes())
    byteBuf.readBytes(bytes)
    val message = new String(bytes, "UTF-8")
    println("ClientHandler的channelRead方法被调用【服务端返回的消息内容为】：" + message)

    //

  }

}
