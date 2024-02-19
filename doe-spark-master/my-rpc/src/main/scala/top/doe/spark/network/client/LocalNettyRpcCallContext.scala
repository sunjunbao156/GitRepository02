package top.doe.spark.network.client

import top.doe.spark.netty.NettyRpcEnv
import top.doe.spark.rpc.RpcCallContext
import top.doe.spark.util.RpcAddress

import scala.concurrent.Promise

abstract class NettyRpcCallContext(override val senderAddress: RpcAddress) extends RpcCallContext {

  protected def send(message: Any): Unit

  //将消费返回给消息的发送者
  override def reply(response: Any): Unit = {
    send(response)
  }

  override def sendFailure(e: Throwable): Unit = ???
}

class LocalNettyRpcCallContext(
  senderAddress: RpcAddress,
  p: Promise[Any]
) extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    p.success(message) //调用Process的success
  }
}

class RemoteNettyRpcCallContext(
  nettyEnv: NettyRpcEnv,
  callback: RpcResponseCallBack,
  senderAddress: RpcAddress
) extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    val reply = nettyEnv.serialize(message)
    //消息出来成功，并且通过回调，将响应的结果返回
    callback.onSuccess(reply)
  }
}
