package top.doe.spark.network

import top.doe.spark.network.client.{RpcResponseCallBack, TransportClient}

import java.nio.ByteBuffer

abstract class RpcHandler {

  //建立上连接后调用
  def channelActive(client: TransportClient): Unit = {}

  //断开连接调用
  def channelInActive(client: TransportClient): Unit = {}

  //出现异常时调用
  def exceptionCaught(e: Throwable, client: TransportClient): Unit = {}

  //接收同步消息，并且指定回调函数
  def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallBack): Unit

  //接收异步消息，指定了一个默认的回调（什么事情都没做，只是打印了log）
  def receive(client: TransportClient, message: ByteBuffer): Unit = {
    receive(client, message, RpcHandler.ONE_WAY_CALLBACK)
  }

}

object RpcHandler {
  private val ONE_WAY_CALLBACK = new OneWayRpcCallback
}

class OneWayRpcCallback extends RpcResponseCallBack {
  override def onSuccess(response: ByteBuffer): Unit = {
    println("Response provided for one-way RPC.")
  }
  override def onFailure(e: Throwable): Unit = {
    println("Error response provided for one-way RPC.", e)
  }
}