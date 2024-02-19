package top.doe.spark.demo.future

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, Future}

object FutureDemo6 {

  def main(args: Array[String]): Unit = {

    def calculateAndReturnNumber(): Int = {
      Thread.sleep(2000)
      val i = 10 / 0
      i
    }

    val futureResult: Future[Any] = Future {
      // 异步计算的逻辑
      // 返回一个 Int 类型的结果
      calculateAndReturnNumber()
    }

    //recover方法可以传入一个偏函数类型，当出现异常会进入到recover中进行匹配
    futureResult.recover{
      case e: Throwable => e.printStackTrace()
    }

    //f2.foreach()

    Thread.sleep(10000)
  }
}