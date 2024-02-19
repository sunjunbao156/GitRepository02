package org.apache.spark.doe

import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.{SecurityManager, SparkConf}

import java.util.concurrent.{Executors, TimeUnit}

class Worker(val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {
  var masterEndpointRef: RpcEndpointRef = _
  val WORKER_ID = "worker02"
  override def onStart(): Unit = {
    //Worker向Master建立连接
    masterEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 8888), "Master")
    //Worker向Master发送的注册消息
    masterEndpointRef.send(RegisterWorker(self, WORKER_ID, 10240, 8))
  }

  override def receive: PartialFunction[Any, Unit] = {

    case RegisteredWorker => {
      //启动一个定时器
      val service = Executors.newScheduledThreadPool(1)
      service.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          masterEndpointRef.send(Heartbeat(WORKER_ID))
        }
      }, 0, 10, TimeUnit.SECONDS)
    }

  }
}

object Worker {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val securityManager = new SecurityManager(conf)
    //1.创建RPCEnv
    val rpcEnv = RpcEnv.create("SparkWorker", "localhost", 9998, conf, securityManager)

    //2.创建RpcEndPoint
    val worker = new Worker(rpcEnv)
    //3.使用RpcEnv注册RpcEndPoint
    rpcEnv.setupEndpoint("Worker", worker)
    //4.将程序挂起，等待退出
    rpcEnv.awaitTermination()


  }

}
