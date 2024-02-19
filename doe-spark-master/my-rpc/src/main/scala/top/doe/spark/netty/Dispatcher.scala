package top.doe.spark.netty

import top.doe.spark.network.client.{LocalNettyRpcCallContext, RemoteNettyRpcCallContext, RpcResponseCallBack}
import top.doe.spark.rpc.{AbortableRpcFuture, RpcEndpoint, RpcEndpointAddress, RpcEndpointRef}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Promise

class Dispatcher(val nettyEnv: NettyRpcEnv, val numUsableCores: Int) {

  //保存RpcEndpoint和RpcEndpointRef的线程安全的Map
  private val endpointRefs = new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]()
  //将指定名称的endpointName和MessageLoop进行关联
  private val endpoints = new ConcurrentHashMap[String, MessageLoop]()
  //用来处理消息的循环线程
  private val sharedLoop = new SharedMessageLoop(this, numUsableCores)

  //将指定的endpoint进行注册（绑定起来）
  //将rpcEndpoint和rpcEndpointRef，并且将rpcEndpoint和对于的消息循环线程进行绑定
  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    val endpointRef = new NettyRpcEndpointRef(addr ,nettyEnv)
    endpointRefs.put(endpoint, endpointRef)
    //将endpoint的名称Inbox进行绑定
    sharedLoop.register(name, endpoint)
    //endpoint名称和MessageLoop进行关联
    endpoints.put(name, sharedLoop)
    endpointRef
  }

  //发送异步消息
  def postOneWayMessage(message: RequestMessage): Unit = {
    //获取接收的endpoint的名称
    val endpointName = message.receiver.name
    val loop = endpoints.get(endpointName)
    loop.post(endpointName, OneWayMessage(message.senderAddress, message.content))
  }

  //发送本地的同步消息
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    //定义一个RpcCallContext，消息发送成功，调用Promise的success方法
    val rpcCallContext = new LocalNettyRpcCallContext(message.senderAddress, p)
    //封装消息内容和rpcCallContext
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    //先找MessageLoop -> Inbox -> RpcEndpoint
    val endpointName = message.receiver.name
    //根据endpoint名称获取消息循序线程对象
    val loop = endpoints.get(endpointName)
    //调用MessageLoop的post方法，将消息投递给Inbox
    loop.post(endpointName, rpcMessage)
  }

  def postToAll(message: InboxMessage): Unit = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next()
      //给指定RpcEndpoint名称的发消息
      postMessage(name, message)
    }
  }

  private def postMessage(endpointName: String, message: InboxMessage): Unit = {
    val loop = endpoints.get(endpointName)
    loop.post(endpointName, message)
  }

  //将远端的消息投递的Inbox中，并指定回调
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallBack): Unit = {
    val rpcCallContext = new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage)
  }

  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }

  def getEndpointRef(rpcEndpoint: RpcEndpoint): RpcEndpointRef = {
    endpointRefs.get(rpcEndpoint)
  }
}
