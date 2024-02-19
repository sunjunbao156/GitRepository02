package top.doe.spark.demo.future

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, Future}

object FutureDemo5 {

  def main(args: Array[String]): Unit = {

    def calculateAndReturnNumber(): Int = {
      Thread.sleep(2000)
      val i = 10 / 2
      i
    }

    val futureResult: Future[Int] = Future {
      // 异步计算的逻辑
      // 返回一个 Int 类型的结果
      calculateAndReturnNumber()
    }
    //同步等待，等待无限大的时间
    //val res: Int = Await.result(futureResult, Duration.Inf)
    //同步等等，最大等待3000毫秒
    val res: Int = Await.result(futureResult, Duration(3000, MILLISECONDS))
    println(res)
  }
}