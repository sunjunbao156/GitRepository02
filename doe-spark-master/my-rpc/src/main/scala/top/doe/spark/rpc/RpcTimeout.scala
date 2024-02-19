package top.doe.spark.rpc

import scala.concurrent.duration.FiniteDuration

class RpcTimeout(val duration: FiniteDuration, val timeoutProp: String) extends Serializable {

  def addMessageIfTimeout[T](): PartialFunction[Throwable, T] = {

    case e: Exception => throw new Exception(e.getMessage + "该同步消息已经超时了！")

  }
}
