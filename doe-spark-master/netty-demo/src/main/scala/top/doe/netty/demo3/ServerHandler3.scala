package top.doe.netty.demo3

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

class ServerHandler3 extends ChannelInboundHandlerAdapter {

  /**
   * 有客户端建立连接后调用
   */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("一个客户端连接上了...")
  }

  /**
   * 接受客户端发送来的消息
   */
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    //进行模式匹配
    msg match {
      case RequestMsg(msg) => {
        println("收到客户端发送的消息：" + msg)
        //将数据发送到客户端
        ctx.writeAndFlush(ResponseMsg("haha"))
      }
    }
  }


}
