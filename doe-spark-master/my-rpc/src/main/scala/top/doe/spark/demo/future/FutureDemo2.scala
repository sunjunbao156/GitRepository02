package top.doe.spark.demo.future

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object FutureDemo2 {

  def main(args: Array[String]): Unit = {

    val f: Future[Int] = Future {
      println("子线程：开始执行计算逻辑，需要运行一定时间...")
      Thread.sleep(2000) //模拟某个耗时操作 比如网络请求
      //val r = 1 / 0
      println("子线程：执行结束，将执行结果返回给Future")
      10
    }
    val c1: Boolean = f.isCompleted
    val v1: Option[Try[Int]] = f.value
    println("future的状态：" + c1)
    v1 match {
      case Some(value) => value match {
        case Success(value) => println("有返回，结果为：" + value)
        case Failure(e) => println("抛出异常：" + e)
      }
      case None => println("无返回，结果为：None")
    }

    println(s"主线程：睡眠3秒...")
    Thread.sleep(3000)

    val c2: Boolean = f.isCompleted
    val v2: Option[Try[Int]] = f.value
    println("future的状态：" + c2)
    v2 match {
      case Some(value) => value match {
        case Success(value) => println("有返回，结果为：" + value)
        case Failure(e) => println("抛出异常：" + e)
      }
      case None => println("无返回，结果为：None")
    }

  }
}
