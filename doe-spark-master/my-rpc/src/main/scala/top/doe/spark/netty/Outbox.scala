package top.doe.spark.netty

import top.doe.spark.network.client.TransportClient
import top.doe.spark.util.RpcAddress

import java.util
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy
import scala.util.control.NonFatal

class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {

  outbox =>

  @GuardedBy("this")
  private var client: TransportClient = null

  @GuardedBy("this")
  private var draining = false

  @GuardedBy("this")
  private val messages = new util.LinkedList[OutBoxMessage]()

  @GuardedBy("this")
  private var stopped: Boolean = false

  @GuardedBy("this")
  private var connectionFuture: java.util.concurrent.Future[Unit] = null

  def send(message: OutBoxMessage): Unit = {
    val dropped = synchronized {
      if (stopped) {
        true
      } else {
        //将要发送的消息，添加到LinkedList中
        messages.add(message)
        false
      }
    }
    if (dropped) {
      message.onFailure(new Exception("消息被丢弃了，应为Outbox已经停止了"))
    } else {
      //将Outbox的消息发送出去
      //清空消息（将要发送的消息使用TransportClient发送出去）
      drainOutbox()
    }
  }

  //判断TranportClient是否别创建，如果已经被创建了，那么直接发送
  private def drainOutbox(): Unit = {
    var message: OutBoxMessage = null
    synchronized {
      if (stopped) {
        return
      }
      if (client == null) {
        //开启一个线程创建TransportClient
        launchConnectTask()
        return
      }
      if (draining) {
        //正在清空发件箱，直接return
        return
      }
      message = messages.poll()
      if (message == null) {
        return
      }
      draining = true
    }
    while (true) {
      try {
        val _client = synchronized {
          client
        }
        if (_client != null) {
          //如果TransportClient已经创建成功了，直接发送消息
          message.sendWith(_client)
        }
      } catch {
        case NonFatal(e) => {
          return
        }
      }
      synchronized {
        if (stopped) {
          return
        }
        message = messages.poll()
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }

  //创建TransportClient（nettyClient）
  def launchConnectTask(): Unit = {
    connectionFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {
      override def call(): Unit = {
        try {
          //创建TransportClient
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            client = _client
          }
        } catch {
          case NonFatal(e) => {
            connectionFuture = null
            return
          }
        }
        outbox.synchronized{
          connectionFuture = null
        }
        //清空Outbox
        drainOutbox()
      }
    })

  }
}
