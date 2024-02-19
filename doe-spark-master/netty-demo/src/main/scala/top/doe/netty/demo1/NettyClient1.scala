package top.doe.netty.demo1

import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel

class NettyClient1 {

  def connect(host: String, port: Int): Unit = {
    //创建客户端NIO线程组
    val eventGroup = new NioEventLoopGroup
    //创建客户端辅助启动类
    val bootstrap = new Bootstrap
    //将NIO线程组传入到Bootstrap
    bootstrap.group(eventGroup)
      //创建NioSocketChannel
      .channel(classOf[NioSocketChannel])
      //绑定事件处理类
      .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline().addLast(new ClientHandler1)
        }
      })
    //发送连接操作
    bootstrap.connect(host, port)
  }
}

object NettyClient1 {
  def main(args: Array[String]) {
    val host = "localhost"
    val port = 8888
    val client = new NettyClient1
    client.connect(host, port)
  }
}
