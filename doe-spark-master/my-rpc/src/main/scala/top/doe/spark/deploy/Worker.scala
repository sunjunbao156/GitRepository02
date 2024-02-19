package top.doe.spark.deploy

import top.doe.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import top.doe.spark.util.RpcAddress

class Worker(
  val rpcEnv: RpcEnv
) extends RpcEndpoint {

  private var masterRpcEndpointRef: RpcEndpointRef = _

  //在onStart中，向Master建立连接
  override def onStart(): Unit = {
    //调用rpcEnv的setupEndpointRef传入远端的地址和端口，返回远端的RpcEndpointRef（连接对象）
    masterRpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 8888), "Master")
    //Worker向Master发送的消息
    masterRpcEndpointRef.send(RegisterWorker(self, "worker01", 1024, 8))
  }

  override def receive: PartialFunction[Any, Unit] = {

    case RegisteredWorker=> {
      println("Worker接收到了Master返回的消息！！！")
    }




  }
}

object Worker {

  def main(args: Array[String]): Unit = {

    val host = "localhost"
    val port = 9999
    //创建RpcEnv
    val rpcEnv = RpcEnv.create("SparkWorker", host, port)
    //创建Worker这个RpcEndpoint
    val worker = new Worker(rpcEnv)
    //使用rpcEnv，将worker进行注册
    rpcEnv.setupEndpoint("Worker", worker)
  }

}
