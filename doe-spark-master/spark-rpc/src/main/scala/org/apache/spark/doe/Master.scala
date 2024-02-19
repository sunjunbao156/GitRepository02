package org.apache.spark.doe

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}

import java.util.concurrent.{Executors, ScheduledThreadPoolExecutor, TimeUnit}
import scala.collection.mutable

class Master(
  val rpcEnv: RpcEnv
) extends ThreadSafeRpcEndpoint {

  val idToWorker = new mutable.HashMap[String, WorkerInfo]()


  override def onStart(): Unit = {
    val executor = new ScheduledThreadPoolExecutor(1)
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        val deadWorkers = idToWorker.values.filter(w => System.currentTimeMillis() - w.lastHeartBeatTime > 10000)
        deadWorkers.foreach(w => {
          idToWorker -= w.workerId
        })
        println(s"当前活跃的Worker数量：${idToWorker.size}")
      }
    }, 0, 15, TimeUnit.SECONDS)
  }

  override def receive: PartialFunction[Any, Unit] = {

    case RegisterWorker(rpcEndpointRef, workerId, workerMemory, workerCores) => {
      val workerInfo = new WorkerInfo(workerId, workerMemory, workerCores)
      //将workerInfo信息保存到map中
      idToWorker(workerId) = workerInfo
      //向Master返回一个注册成功的消息
      rpcEndpointRef.send(RegisteredWorker)
    }

    case Heartbeat(workerId) => {
      val workerInfo = idToWorker(workerId)
      //跟新最后一次访问时间
      workerInfo.lastHeartBeatTime = System.currentTimeMillis()
    }
  }

  //接送同步消息的方法
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case "ask-smg" =>
      println("Master接收到了Worker发送的同步消息了")
      //向Worker返回消息
      Thread.sleep(3000)
      //Master响应Worker的消息
      context.reply("reply-msg")

  }
}

object Master {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val securityManager = new SecurityManager(conf)
    //1.创建RPCEnv
    val rpcEnv = RpcEnv.create("SparkMaster", "localhost", 8888, conf, securityManager)

    //2.创建RpcEndPoint
    val master = new Master(rpcEnv)
    //3.使用RpcEnv注册RpcEndPoint
    val masterEndpointRef = rpcEnv.setupEndpoint("Master", master)

    //4.通过返回的RpcEndPointRef，发送消息
    //masterEndpointRef.send("test")

    //5.将程序挂起，等待退出
    rpcEnv.awaitTermination()
  }

}
