package top.doe.spark.demo.future

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object FutureDemo4 {

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

    //如果抛出异常，不会执行map或foreach中的函数不会执行
    //map和foreach的区别，map方法还会返回Future，foreach没有返回
    val f2 = futureResult.map(res => {
      val r = res * 10
      println(r)
      r
    })

    Thread.sleep(10000)
  }
}