package top.doe.spark.demo.future

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object FutureDemo3 {



  def main(args: Array[String]): Unit = {

    def calculateAndReturnNumber(): Int = {
      //模拟执行一些逻辑，sleep 2秒
      Thread.sleep(2000)
      val i = 10 / 0
      i
    }

    //启动子线程
    val futureResult: Future[Int] = Future {
      // 异步计算的逻辑
      // 返回一个 Int 类型的结果
      calculateAndReturnNumber()
    }

    //当future中有返回或出现异常，使用偏函数继续回调并匹配
    futureResult.onComplete {
      case Success(result) => println(s"returned number is $result")
      case Failure(e) => println(s"failed with exception $e")
    }

    //主线程不能退出，不然回调方法无法调用
    Thread.sleep(5000)
  }
}