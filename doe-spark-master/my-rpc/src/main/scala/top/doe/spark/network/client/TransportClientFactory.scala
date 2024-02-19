package top.doe.spark.network.client

import io.netty.bootstrap.Bootstrap
import io.netty.channel.{Channel, ChannelFuture, ChannelInitializer, ChannelOption, EventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import top.doe.spark.network.TransportContext
import top.doe.spark.network.server.TransportChannelHandler

import java.io.{Closeable, IOException}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference

class TransportClientFactory(val context: TransportContext) extends Closeable{

  //客户端线程池组
  var workerGroup: EventLoopGroup = new NioEventLoopGroup()

  //使用TransportClientFactory创建TransportClient（NettyClient）
  def createClient(remoteHost: String, remotePort: Int): TransportClient = {
    val address = new InetSocketAddress(remoteHost, remotePort)
    //启动客户端辅助工具类
    val bootstrap = new Bootstrap
    bootstrap.group(workerGroup)
      //指定Channel
      .channel(classOf[NioSocketChannel])
      //设置相关参数
      .option(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
      .option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, java.lang.Integer.valueOf(120000))

    //可以在多线程环境下原子性地操作引用对象，保证线程安全
    val clientRef: AtomicReference[TransportClient] = new AtomicReference[TransportClient]
    val channelRef: AtomicReference[Channel] = new AtomicReference[Channel]

    //指定Handler处理器
    bootstrap.handler(new ChannelInitializer[SocketChannel]() {
      override def initChannel(ch: SocketChannel): Unit = {
        val clientHandler: TransportChannelHandler = context.initializePipeline(ch)
        clientRef.set(clientHandler.getClient)
        channelRef.set(ch)
      }
    })
    //建立连接
    val cf: ChannelFuture = bootstrap.connect(address)

    if (!cf.await(120000))
      throw new IOException(String.format("Connecting to %s timed out (%s ms)", address, "120000"))
    else if (cf.cause != null)
      throw new IOException(String.format("Failed to connect to %s", address), cf.cause)

    //返回连接对象
    val client = clientRef.get
    client
  }

  override def close(): Unit = {
    if (workerGroup != null && !workerGroup.isShuttingDown)
      workerGroup.shutdownGracefully
  }

}
