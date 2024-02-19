package top.doe.spark.netty

import top.doe.spark.network.client.NettyRpcCallContext
import top.doe.spark.rpc.RpcEndpoint
import top.doe.spark.util.RpcAddress

import java.util

//投递到收件箱的消息
sealed trait InboxMessage

//Inbox在实例化的时候会给自己发送的消息，就是实现了生命周期方法
case object OnStart extends InboxMessage

//发送到Inbox中的异步消息
case class OneWayMessage(
  senderAddress: RpcAddress,
  content: Any
) extends InboxMessage

//同步类型消息
case class RpcMessage(
  senderAddress: RpcAddress,
  content: Any,
  context: NettyRpcCallContext
) extends InboxMessage

case class RemoteProcessConnected(rpcAddress: RpcAddress) extends InboxMessage

class Inbox(val endpointName: String, val endpoint: RpcEndpoint) {

  inbox =>

  private val messages = new util.LinkedList[InboxMessage]()

  inbox.synchronized {
    messages.add(OnStart)
  }

  def post(message: InboxMessage): Unit = inbox.synchronized {
    messages.add(message)
  }

  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    inbox.synchronized {
      message = messages.poll()
      if (message == null) {
        return
      }
    }
    //while (true) {

    //对消息进行模式匹配
    message match {
      case OnStart =>
        //调用对应的RpcEndpoint的生命周期方法
        endpoint.onStart()

      //异步的消息
      case OneWayMessage(senderAddress, content) =>
        endpoint.receive.apply(content)

      //同步的消息
      case RpcMessage(senderAddress, content, context) =>
        endpoint.receiveAndReply(context).apply(content)

      //有连接建立后，接收到的消息
      case RemoteProcessConnected(rpcAddress) => {
        endpoint.onConnected(rpcAddress)
      }
    }


    //}


  }

  //自己给自己发OnStart


}
