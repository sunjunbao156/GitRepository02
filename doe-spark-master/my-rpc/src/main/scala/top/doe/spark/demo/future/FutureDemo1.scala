package top.doe.spark.demo.future

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

object FutureDemo1 {

  def main(args: Array[String]): Unit = {

    val f: Future[Int] = Future {
      println("子线程：开始执行计算逻辑，需要运行一定时间...")
      Thread.sleep(2000) //模拟某个耗时操作 比如网络请求
      println("子线程：执行结束，将执行结果返回给Future")
      val i = 1 / 0
      10
    }
    val c1: Boolean = f.isCompleted
    val v1: Option[Try[Int]] = f.value

    println(s"主线程：future的状态：$c1, future中的数据：$v1")
    println(s"主线程：睡眠3秒...")
    Thread.sleep(3000)

    val c2: Boolean = f.isCompleted
    val v2: Option[Try[Int]] = f.value
    println(s"主线程：future的状态：$c2, future中的数据：$v2")
  }
}
