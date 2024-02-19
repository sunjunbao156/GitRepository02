package top.doe.spark.netty

import top.doe.spark.network.client.{RpcResponseCallBack, TransportClient, TransportClientFactory}
import top.doe.spark.network.server.TransportServer
import top.doe.spark.network.{RpcHandler, TransportContext}
import top.doe.spark.rpc.{AbortableRpcFuture, RpcEndpoint, RpcEndpointAddress, RpcEndpointRef, RpcEnv, RpcTimeout}
import top.doe.spark.serializer.{JavaSerializerInstance, SerializationStream, SerializerInstance}
import top.doe.spark.util.RpcAddress

import java.io.OutputStream
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise, TimeoutException}
import scala.reflect.ClassTag
import scala.util._




class NettyRpcEnv(
  val numUsableCores: Int,
  javaSerializerInstance: SerializerInstance //新添加的
) extends RpcEnv {

  var server: TransportServer = _
  lazy val address: RpcAddress = if(server != null) RpcAddress(server.host, server.port) else null
  private val timeoutScheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  private val dispatcher = new Dispatcher(this, numUsableCores)
  private val transportContext = new TransportContext(new NettyRpcHandler(dispatcher, this))
  //将RpcAddress和Outbox进行绑定
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  val clientConnectionExecutor = Executors.newSingleThreadExecutor()
  val clientFactory: TransportClientFactory = transportContext.createClientFactory()

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  def getDispatcher(): Dispatcher = {
    this.dispatcher
  }

  def startServer(bindAddress: String, port: Int) = {
    server = transportContext.createServer(bindAddress, port)
  }

  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getEndpointRef(endpoint)
  }

  //异步向Master发送与远端的RpcEndpoint建立连接的请求（获取远端RpcEndpointRef）
  //uri 地址的样子 spark://Master@localhost:8888
  override def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    //向指定的RpcEndpointRef发送一个ask请求，到远端的RpcEnv中查找对应名称的RpcEndpoint，并且返回其RpcEndpointRef
    val addr = RpcEndpointAddress(uri)
    //未来要连接的Master的引用
    val rpcEndpointRef = new NettyRpcEndpointRef(addr, this) //rpcEndpointRef.name 对应的就是Master
    rpcEndpointRef.ask[Boolean](CheckExistence(rpcEndpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(rpcEndpointRef)
      } else {
        Future.failed(new Exception("指定的Endpoint没有找到，对应的地址是：" + uri))
      }
    }
  }

  //将消息发送出去：1.内部消息 2.远端的消息
  def send(message: RequestMessage): Unit = {
    val remoteAddr = message.receiver.address
    if(remoteAddr == address) {
      //本地的内部消息
      dispatcher.postOneWayMessage(message)
    } else {
      //远端的异步消息，就是将消息放入到OutBox中
      postToOutbox(message.receiver, OneWayOutboxMessage(message.serialize(this)))
    }
  }

  //发送同步消息，消息发送成功有回调，发送失败也要有回调
  def askAbortable[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    val promise = Promise[Any]()
    //获取远端的地址
    val remoteAddr = message.receiver.address

    def onSuccess(reply: Any): Unit = {
      //将商品的promise设置为成功状态
      if(!promise.trySuccess(reply)) {
        println("ignored message: " + reply)
      }
    }

    def onFailure(e: Throwable): Unit = {
      if(!promise.tryFailure(e)) {
        println("ignored failure: " + e)
      }
    }

    if(remoteAddr == address) {
      //本地的同步消息
      val p = Promise[Any]()
      p.future.onComplete{
        case Success(response) =>  onSuccess(response)
        case Failure(e) => onFailure(e)
      }
      //通过dispatcher发送本地消息
      dispatcher.postLocalMessage(message, p)
    } else {
      //远端的同步消息
      val rpcMessage = RpcOutboxMessage(
        message.serialize(this),
        onFailure,
        (client, response) => onSuccess(deserialize[Any](client, response))
      )
      postToOutbox(message.receiver, rpcMessage)
    }

    //启动定时器，进程在指定的时间内，消息是否成功被调用
    val timeoutCancelable = timeoutScheduler.schedule(
      new Runnable {
        override def run(): Unit = {
          onFailure(new TimeoutException("消息请求超时，超时的时间：" + timeout.duration))
        }
      },
      timeout.duration.toMillis, //拿出来设置好的超时时间
      TimeUnit.MILLISECONDS //时间单位
    )
    promise.future.onComplete{ v =>
      timeoutCancelable.cancel(true)
    }

    //返回AbortableRpcFuture，里面持有着Future
    new AbortableRpcFuture[T](promise.future.mapTo[T].recover(timeout.addMessageIfTimeout), onFailure)
  }

  //将准备好的消息放到OutBox中
  def postToOutbox(receiver: NettyRpcEndpointRef, message: OutBoxMessage): Unit = {
    //如果TransportClient不为空，说明链接已经建立好了
    if(receiver.client != null) {
      message.sendWith(receiver.client)
    } else {
      val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oloOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oloOutbox == null) {
            newOutbox
          } else {
            oloOutbox
          }
        } else {
          outbox
        }
      }
      //将要发送的消息，放到对应的Outbox中，准备发送
      targetOutbox.send(message)
    }
  }

  def createClient(address: RpcAddress): TransportClient = {
    clientFactory.createClient(address.host, address.port)
  }

  def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize(content)
  }

  def deserialize[T](deserializationAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserializationAction()
    }
  }

  def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () =>
        javaSerializerInstance.deserialize[T](bytes)
      }
    }
  }

  def serializeStream(out: OutputStream): SerializationStream = {
    javaSerializerInstance.serializeStream(out)
  }

}

object NettyRpcEnv {
  //DynamicVariable ，这个类有点类似于 java 里面 ThreadLocal ，每个线程都拥有它自己的副本变量的值
  val currentEnv = new DynamicVariable[NettyRpcEnv](null)
  val currentClient = new DynamicVariable[TransportClient](null)

}


class NettyRpcHandler(
  dispatcher: Dispatcher,
  nettyEnv: NettyRpcEnv,
) extends RpcHandler {

  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  //方法稍后实现...
  override def channelActive(client: TransportClient): Unit = {
    val addr = client.channel.remoteAddress().asInstanceOf[InetSocketAddress]
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    dispatcher.postToAll(RemoteProcessConnected(clientAddr))
  }

  //接收同步的消息
  override def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallBack): Unit = {
    val messageToDispatch = internalReceive(client, message)
    //向Dispatcher向Inbox投递一个远端的消息，并且知道回调函数
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }

  //接受异步的消息
  override def receive(client: TransportClient, message: ByteBuffer): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postOneWayMessage(messageToDispatch)
  }

  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage = {
    val addr = client.channel.remoteAddress().asInstanceOf[InetSocketAddress]
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    val requestMessage = RequestMessage(nettyEnv, client, message)
    if (requestMessage.senderAddress == null) {
      new RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else {
      val remoteAddr = requestMessage.senderAddress
      //如果只知道地址的客户端第一次发生的消息
      if (remoteAddresses.putIfAbsent(clientAddr, remoteAddr) == null) {
        //就向当前RpcEnv中所有的RpcEndpoint发送一个RemoteProcessConnected消息
        dispatcher.postToAll(RemoteProcessConnected(remoteAddr))
      }
      requestMessage
    }



  }
}
