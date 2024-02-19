package top.doe.spark.demo.promise


import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util._

object PromiseDemo2 {

  def doQuery(p: Promise[Any]): Unit = {
    Thread.sleep(2000)
    val random = new Random()
    val res = random.nextInt(3)
    println("random返回的结果为：" + res)
    if (res > 0) {
      p.success(res)
    } else {
      p.failure(new Exception("有异常了"))
    }
  }

  def main(args: Array[String]): Unit = {

    val p: Promise[Any] = Promise[Any]()

    p.future.onComplete {
      case Success(value) => println("success: " + value)
      case Failure(e) => println("failure: " + e)
    }
    //开启一个子线程，执行doQuery方法，将Promise对象传进去
    Future {
      //查询消耗一定的时间
      doQuery(p)
    }

    Thread.sleep(5000)

  }

}
