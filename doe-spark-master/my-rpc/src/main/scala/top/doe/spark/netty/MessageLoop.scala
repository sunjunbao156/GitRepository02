package top.doe.spark.netty

import top.doe.spark.rpc.RpcEndpoint

import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, LinkedBlockingQueue}


abstract class MessageLoop(dispatcher: Dispatcher) {

  //用了保存已经准备好的Inbox的阻塞队列
  private val active = new LinkedBlockingQueue[Inbox]()

  //将创建的好的Inbox添加到阻塞队列中
  def setActive(inbox: Inbox): Unit = active.offer(inbox)

  //将消息发送给指定的endpoint（根据名字找到对应Inbox，inbox中持有RpcEndpoint）
  def post(endpointName: String, message: InboxMessage): Unit

  protected val receiveLoopRunnable = new Runnable {
    override def run(): Unit = receiveLoop()
  }

  private def receiveLoop(): Unit = {
    while (true) {
      //从阻塞队列中取出准备好的Inbox对象
      val inbox = active.take()
      inbox.process(dispatcher)
    }

  }

}

class SharedMessageLoop(dispatcher: Dispatcher, numUsableCores: Int) extends MessageLoop(dispatcher) {

  //用来保存指定rpcEndpoint名称和Inbox线程安全的map
  private val endpoints = new ConcurrentHashMap[String, Inbox]()

  //初始化线程池
  private val pool: ExecutorService = Executors.newFixedThreadPool(numUsableCores)
  for(i <- 0 until numUsableCores) {
    //提交线程对象
    pool.execute(receiveLoopRunnable)
  }

  //将指定的RpcEndpoint和Inbox进行绑定
  def register(name: String, endpoint: RpcEndpoint): Unit = {
    val inbox = new Inbox(name, endpoint)
    endpoints.put(name, inbox) //将Endpoint的名称和对应的Inbox保存的了endpoints
    setActive(inbox)
  }

  override def post(endpointName: String, message: InboxMessage): Unit = {
    val inbox = endpoints.get(endpointName)
    //将消息添加到Inbox的集合中
    inbox.post(message)
    setActive(inbox)
  }
}
