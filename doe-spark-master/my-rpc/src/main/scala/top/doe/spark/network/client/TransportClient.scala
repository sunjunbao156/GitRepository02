package top.doe.spark.network.client

import io.netty.channel.Channel
import io.netty.util.concurrent.{Future, GenericFutureListener}
import top.doe.spark.netty.RpcOutboxMessage
import top.doe.spark.network.buffer.NioManagedBuffer
import top.doe.spark.network.protocol.{OneWayMessage, RpcRequest}

import java.io.IOException
import java.nio.ByteBuffer
import java.util.UUID

class TransportClient(
  val channel: Channel,
  val handler: TransportResponseHandler
) {

  private def requestId(): Long = Math.abs(UUID.randomUUID().getLeastSignificantBits)

  //发送同步消息，并且指定回调函数
  def sendRpc(content: ByteBuffer, callBack: RpcOutboxMessage): Long = {
    val reqId = requestId()
    handler.addRpcRequest(reqId, callBack)
    //创建一个监听器，用来监听肯客户端的消息是否发生成功
    val listener = new RpcChannelListener(reqId, callBack)
    channel.writeAndFlush(new RpcRequest(reqId, new NioManagedBuffer(content))).addListener(listener)
    reqId
  }

  //发送异步的消息
  def send(content: ByteBuffer): Unit = {
    channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(content)))
  }

  private class RpcChannelListener(
    val rpcRequestId: Long,
    var callback: BaseResponseCallBack
  ) extends GenericFutureListener[Future[_ >: Void]] {

    //消息发送完成，不论成功还是失败，都会调用operationComplete方法
    override def operationComplete(future: Future[_ >: Void]): Unit = {
      if(future.isSuccess) {
        println("消息发送成功，请求的ID是：" + rpcRequestId)
      } else {
        channel.close()
        //将该请求的ID对应的回调函数移除
        handler.removeRpcRequest(rpcRequestId)
        //调用失败的回调
        callback.onFailure(new IOException("发现发送失败，请求的ID是：" + rpcRequestId, future.cause()))
      }

    }
  }

}
