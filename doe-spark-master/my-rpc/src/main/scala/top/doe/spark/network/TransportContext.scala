package top.doe.spark.network

import io.netty.channel.Channel
import io.netty.channel.socket.SocketChannel
import top.doe.spark.network.TransportContext.{DECODER, ENCODER}
import top.doe.spark.network.client.{TransportClient, TransportClientFactory, TransportResponseHandler}
import top.doe.spark.network.protocol.{MessageDecoder, MessageEncoder}
import top.doe.spark.network.server.{TransportChannelHandler, TransportRequestHandler, TransportServer}
import top.doe.spark.network.util.TransportFrameDecoder

class TransportContext(val rpcHandler: RpcHandler) {

  //创建TransportServer（NettyServer）
  def createServer(host: String, port: Int) = new TransportServer(host, port, rpcHandler, this)

  //创建TransportClientFactory（创建NettyClient的工厂）
  def createClientFactory(): TransportClientFactory = new TransportClientFactory(this)

  //初始化Netty的pipeline
  def initializePipeline(channel: SocketChannel): TransportChannelHandler = {
    initializePipeline(channel, rpcHandler)
  }

  def initializePipeline(channel: SocketChannel, rpcHandler: RpcHandler): TransportChannelHandler = {
    //调用createChannelHandler绑定自定义的Handler
    val channelHandler: TransportChannelHandler = createChannelHandler(channel, rpcHandler)
    val pipeline = channel.pipeline
    pipeline
      //发送数据先使用Spark的序列化器进行序列化，然后再调用 encoder 对序列化的数据按照Netty传输数据规范进行编码
      .addLast("encoder", ENCODER) //实现了ChannelOutboundHandler
      //处理请求的消息，会依次调用 frameDecoder -> decoder -> channelHandler
      .addLast("frameDecoder", new TransportFrameDecoder()) //实现了ChannelInboundHandler
      .addLast("decoder", DECODER) //实现了ChannelInboundHandler
      .addLast("handler", channelHandler) //实现ChannelInboundHandler

    channelHandler
  }

  private def createChannelHandler(channel: Channel, rpcHandler: RpcHandler): TransportChannelHandler = {
    //创建用来响应请求的Handler
    val responseHandler = new TransportResponseHandler(channel)
    val client = new TransportClient(channel, responseHandler)
    //创建用来处理请求的Handler
    val requestHandler = new TransportRequestHandler(channel, client, rpcHandler)
    //创建TransportChannelHandler并将处理请求和响应的Handler传入到其构造方法中
    new TransportChannelHandler(client, responseHandler, requestHandler)
  }

}

object TransportContext {

  //用于将消息进行序列化的Encoder
  private val ENCODER = MessageEncoder.INSTANCE
  //用于将消息进行反序列化的Decoder
  private val DECODER = MessageDecoder.INSTANCE


}
