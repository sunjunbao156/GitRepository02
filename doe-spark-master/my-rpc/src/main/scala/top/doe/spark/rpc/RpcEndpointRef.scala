package top.doe.spark.rpc

import top.doe.spark.util.RpcAddress

import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

abstract class RpcEndpointRef extends Serializable {

  def name: String

  def address: RpcAddress

  //发送异步消息
  def send(message: Any): Unit

  //发送同步消息,并指定超时时间，T代表请求后返回的消息类型
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  def ask[T: ClassTag](message: Any): Future[T] = ask(message, new RpcTimeout(Duration(10, TimeUnit.SECONDS), "RpcTimeout"))

  //发送同步请求，超时就放弃，可以抛出异常
  def askAbortable[T: ClassTag](message: Any, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    throw new RuntimeException("不支持的请求")
  }

}

//封装Future对象，并且出现异常的情况，调用调用外部出入的异常处理函数
class AbortableRpcFuture[T: ClassTag](val future: Future[T], onAbort: Throwable => Unit) {
  def abort(t: Throwable): Unit = onAbort(t)
}