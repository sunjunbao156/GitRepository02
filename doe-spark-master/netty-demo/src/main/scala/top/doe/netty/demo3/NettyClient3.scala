package top.doe.netty.demo3

import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.serialization.{ClassResolvers, ObjectDecoder, ObjectEncoder}

class NettyClient3 {

  def connect(host: String, port: Int): Unit = {
    //创建客户端NIO线程组
    val eventGroup = new NioEventLoopGroup
    //创建客户端辅助启动类
    val bootstrap = new Bootstrap
    //将NIO线程组传入到Bootstrap
    bootstrap.group(eventGroup)
      //创建NioSocketChannel
      .channel(classOf[NioSocketChannel])
      //绑定I/O事件处理类
      .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline().addLast("encoder", new ObjectEncoder)
          ch.pipeline().addLast("decoder", new ObjectDecoder(ClassResolvers.cacheDisabled(getClass.getClassLoader)))
          ch.pipeline().addLast("handler", new ClientHandler3)
        }
      })
    //发起异步连接操作
    val channelFuture = bootstrap.connect(host, port)
    channelFuture.syncUninterruptibly
  }
}

object NettyClient3 {
  def main(args: Array[String]) {
    val host = "localhost"
    val port = 8888
    val client = new NettyClient3
    client.connect(host, port)
  }
}
