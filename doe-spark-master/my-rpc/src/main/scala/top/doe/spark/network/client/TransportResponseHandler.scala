package top.doe.spark.network.client

import io.netty.channel.Channel
import top.doe.spark.network.protocol.{ResponseMessage, RpcResponse}

import java.util.concurrent.ConcurrentHashMap

class TransportResponseHandler(
  val channel: Channel
) {

  private val outstandingRpcs = new ConcurrentHashMap[Long, BaseResponseCallBack]()

  def channelActive(): Unit = {}

  def addRpcRequest(requestId: Long, callBack: RpcResponseCallBack): Unit = {
    outstandingRpcs.put(requestId, callBack)
  }

  def removeRpcRequest(requestId: Long): Unit = {
    outstandingRpcs.remove(requestId)
  }

  def handle(message: ResponseMessage): Unit = {
    message match {
      case resp: RpcResponse => {
        //获取服务端返回的内容（返回的请求id）
        val requestId = resp.requestId
        val listener = outstandingRpcs.get(requestId).asInstanceOf[RpcResponseCallBack]
        if(listener == null) {
          resp.body().release()
        } else {
          outstandingRpcs.remove(requestId)
          try {
            listener.onSuccess(resp.body().nioByteBuffer())
          } finally {
            resp.body().release()
          }
        }
      }
      case _ =>
    }


  }

}
