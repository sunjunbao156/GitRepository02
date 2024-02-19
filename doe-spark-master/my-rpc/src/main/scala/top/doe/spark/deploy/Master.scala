package top.doe.spark.deploy


import top.doe.spark.netty.{CheckExistence, NettyRpcEnv}
import top.doe.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util._

class Master(
  val rpcEnv: RpcEnv
) extends RpcEndpoint {

  override def onStart(): Unit = {
    println("onStart方法被调用了！！！")
  }

  //接送消息，并进行模式匹配，然后调用相应的方法
  override def receive: PartialFunction[Any, Unit] = {
    case "test" => {
      println(666)
    }

    case "register" => {
      println("接收到了Worker发送过来的消息了！！！")

    }

    case RegisterWorker(rpcEndpointRef, workerId, workerMemory, workerCores) => {
      println(s"接收到了Worker发送的消息：workerId：$workerId, workerMemory: $workerMemory, workerCores: $workerCores")
      //将Worker的信息保存起来
      //向Worker返回一个注册成功的消息
      rpcEndpointRef.send(RegisteredWorker)
    }
  }

  //接受同步消息，并且返回响应
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    //Worker发送给Master的消息，目的是检测Master在RpcEnv中是否已经注册过了
    case CheckExistence(name) => {
      val flag: Boolean = rpcEnv.asInstanceOf[NettyRpcEnv].getDispatcher().verify(name)
      context.reply(flag)
    }

    case "sync_msg" => {
      //执行相应的逻辑
      //...
      Thread.sleep(3000)
      //然后给消息发送者返回消息
      context.reply("response_msg")
    }

  }
}

object Master {

  def main(args: Array[String]): Unit = {

    val host = "localhost"
    val port = 8888
    //1.创建RpcEnv
    val rpcEnv = RpcEnv.create("SparkMaster", host, port)

    //2.创建RpcEndpoint（Master）
    val master = new Master(rpcEnv)

    //3.将创建好的RpcEndpoint使用RpcEnv进行注册
    val endpointRef: RpcEndpointRef = rpcEnv.setupEndpoint("Master", master)

    //4.自己给自己发消息
    //endpointRef.send("test")

    //5.发送同步的消息
    val future: Future[String] = endpointRef.ask[String]("sync_msg")

    //    future.foreach(res => {
    //      println("同步消息返回的内容：" + res)
    //    })

    future.onComplete {
      case Success(value) => println("正常返回的消息：" + value)
      case Failure(exception) => println("请求超时，返回的异常" + exception)
    }

  }

}
