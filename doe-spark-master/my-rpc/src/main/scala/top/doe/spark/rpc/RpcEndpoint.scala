package top.doe.spark.rpc

import top.doe.spark.util.RpcAddress

trait RpcEndpoint {

  val rpcEnv: RpcEnv

  //获取自己的引用
  final def self: RpcEndpointRef = {
    rpcEnv.endpointRef(this)
  }

  def onStart(): Unit = {

  }

  def onConnected(rpcAddress: RpcAddress): Unit = {}

  //异步接收消息的方法
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new Exception("没有匹配上任何消息类型！")
  }

  //同步接受消息的方法
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new Exception("receiveAndReply没有匹配上任何消息"))
  }

}
