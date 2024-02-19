package top.doe.spark.netty

import top.doe.spark.network.client.{RpcResponseCallBack, TransportClient}

import java.nio.ByteBuffer

sealed trait OutBoxMessage {

  def sendWith(client: TransportClient): Unit

  def onFailure(e: Throwable): Unit

}

//发送到远端的同步消息
case class RpcOutboxMessage(
  content: ByteBuffer,
  _onFailure: (Throwable) => Unit,
  _onSuccess: (TransportClient, ByteBuffer) => Unit
) extends OutBoxMessage with RpcResponseCallBack {

  var client: TransportClient = _
  var requestId: Long = _

  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    this.requestId = client.sendRpc(content, this) //调用TransportClient发送同步消息的方法
  }

  override def onSuccess(response: ByteBuffer): Unit = {
    _onSuccess(client, response)
  }

  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }
}

case class OneWayOutboxMessage(content: ByteBuffer) extends OutBoxMessage {

  override def sendWith(client: TransportClient): Unit = {
    //发送异步消息
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    println("发送异步消息失败")
  }
}
