package top.doe.spark.rpc

import top.doe.spark.netty.NettyRpcEnvFactory
import top.doe.spark.util.RpcAddress

import java.util.concurrent.TimeUnit
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

abstract class RpcEnv {

  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  //向远端的服务端发送异步请求，返回Future
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  //根据远端的RpcEndpoint的URI地址，和名称，获取对应的RpcEndpointRef
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    //同步的获取Future中的远端的RpcEndpointRef（连接对象）
    Await.result(asyncSetupEndpointRefByURI(uri), Duration.apply(30, TimeUnit.SECONDS))
  }

  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }
}

object RpcEnv {

  def create(name: String, host: String, port: Int): RpcEnv = {
    val config = RpcEnvConfig(name, host, port, 1)
    new NettyRpcEnvFactory().create(config)
  }

}

case class RpcEnvConfig(
  name: String,        //RpcEnv的名称
  bindAddress: String, //RpcEnv绑定的地址
  port: Int,           //RpcEnv绑定的端口
  numUsableCores: Int  //消息循环线程池中线程对象的数量
)
