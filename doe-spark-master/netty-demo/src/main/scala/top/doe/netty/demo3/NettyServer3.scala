package top.doe.netty.demo3

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.serialization.{ClassResolvers, ObjectDecoder, ObjectEncoder}

class NettyServer3 {

  def bind(host: String, port: Int): Unit = {
    //配置服务端线程池组
    //用于服务器接收客户端的连接
    val bossGroup = new NioEventLoopGroup()
    //用户进行SocketChannel的网络读写
    val workerGroup = new NioEventLoopGroup()

    //是Netty用户启动NIO服务端的辅助启动类，降低服务端的开发复杂度
    val bootstrap = new ServerBootstrap()
    //将两个NIO线程组作为参数传入到ServerBootstrap
    bootstrap.group(bossGroup, workerGroup)
      //创建NioServerSocketChannel
      .channel(classOf[NioServerSocketChannel])
      //绑定I/O事件处理类
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          //处理输入的数据执行顺序 decoder -> handler
          //处理返回的数据执行顺序 encoder
          ch.pipeline().addLast("encoder", new ObjectEncoder) //实现了ChannelOutboundHandler
          ch.pipeline().addLast("decoder", new ObjectDecoder(ClassResolvers.cacheDisabled(getClass.getClassLoader))) //实现了ChannelInboundHandler
          ch.pipeline().addLast("handler", new ServerHandler3) //实现了ChannelInboundHandler
        }
      })
    val channelFuture = bootstrap.bind(host, port)
    channelFuture.syncUninterruptibly
  }
}

object NettyServer3 {
  def main(args: Array[String]) {
    val host = "localhost"
    val port = 8888
    val server = new NettyServer3
    server.bind(host, port)
  }
}
