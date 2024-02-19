package top.doe.spark.rpc

import top.doe.spark.util.RpcAddress

trait RpcCallContext {

  //给消息的发送者返回消息
  def reply(response: Any): Unit

  //标记消费发生失败，将异常传入
  def sendFailure(e: Throwable): Unit

  //获取消息发送者的地址
  def senderAddress: RpcAddress
}
