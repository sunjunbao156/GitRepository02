package top.doe.spark.network.server

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelInitializer, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import top.doe.spark.network.{RpcHandler, TransportContext}

import java.net.InetSocketAddress

class TransportServer(
  val host: String,
  val port: Int,
  val rpcHandler: RpcHandler,
  val context: TransportContext
) {

  //肯定要创建Netty的服务端
  init(host, port)

  def init(host: String, port: Int): Unit = {
    //创建Netty的服务端
    val bossGroup = new NioEventLoopGroup()
    val workerGroup = new NioEventLoopGroup()
    //创建一个启动工具类
    val bootstrap = new ServerBootstrap()
    bootstrap.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .option(ChannelOption.SO_REUSEADDR, java.lang.Boolean.TRUE)

    bootstrap.childHandler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        context.initializePipeline(ch, rpcHandler)
      }
    })
    //启动netty的服务端，绑定地址和端口，运行起来
    bootstrap.bind(new InetSocketAddress(host, port))
  }

}
