package top.doe.netty.demo3

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}


class ClientHandler3 extends ChannelInboundHandlerAdapter {

  //一旦跟服务端建立上连接channelActive将被调用
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("已经跟服务端连接上了")
    //向服务端发送case class实例
    ctx.writeAndFlush(RequestMsg("hello"))
  }


  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    msg match {
      case ResponseMsg(msg) => {
        println("收到服务端返回的消息：" + msg)
      }
    }
  }
}
