package top.doe.netty.demo1

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel

class NettyServer1 {

  def bind(host: String, port: Int): Unit = {
    //用于接收客户端的连接请求的线程池
    val bossGroup = new NioEventLoopGroup()
    //用与处理客户端SocketChannel的网络读写的线程池
    val workerGroup = new NioEventLoopGroup()
    //是Netty用户启动NIO服务端的辅助启动类，降低服务端的开发复杂度
    val bootstrap = new ServerBootstrap()
    //将两个NIO线程组作为参数传入到ServerBootstrap
    bootstrap.group(bossGroup, workerGroup)
      //创建NioServerSocketChannel
      .channel(classOf[NioServerSocketChannel])
      //绑定事件处理类
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline().addLast(new ServerHandler1)
        }
      })
    //绑定端口地址端口
    bootstrap.bind(host, port)
  }
}

object NettyServer1 {

  def main(args: Array[String]) {
    val host = "localhost"
    val port = 8888
    val server = new NettyServer1
    server.bind(host, port)
  }
}
