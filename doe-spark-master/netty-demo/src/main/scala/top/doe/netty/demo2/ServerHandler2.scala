package top.doe.netty.demo2

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

class ServerHandler2 extends ChannelInboundHandlerAdapter {

  /**
   * 有客户端建立连接后调用
   */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("ServerHandler的channelActive方法被调用【一个客户端连接上】")
  }

  /**
   * 接受客户端发送来的消息
   */
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {

    //将接收的数据转成字符串，然后打印
    val byteBuf = msg.asInstanceOf[ByteBuf]
    val bytes = new Array[Byte](byteBuf.readableBytes())
    byteBuf.readBytes(bytes)
    val message = new String(bytes, "UTF-8")
    println("ServerHandler的channelRead方法被调用【收到客户端发送的消息内容为】：" + message)

    //将数据发送到客户端
    val back = "你好"
    val resp = Unpooled.copiedBuffer(back.getBytes("UTF-8"))
    ctx.writeAndFlush(resp)
  }

}
