package top.doe.spark.demo.promise

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util._

object PromiseDemo1 {

  def doQuery(): Int = {
    Thread.sleep(2000)
    val random = new Random()
    val res = random.nextInt(3)
    println("doQuery返回的结果为：" + res)
    res
  }

  def main(args: Array[String]): Unit = {

    val p: Promise[Any] = Promise[Any]()
    val f: Future[Any] = p.future

    //开启一个子线程
    Future {
      //查询消耗一定的时间
      val res = doQuery()
      if (res > 0) {
        p.success(res)
      } else {
        p.failure(new RuntimeException("出现错误了，抛出异常"))
      }
    }

    f.onComplete {
      case Success(value) => println("success: " + value)
      case Failure(e) => println("failure: " + e)
    }

    Thread.sleep(5000)
  }

}
