package top.doe.spark.network.client

//以后发生一次调用的方法
trait BaseResponseCallBack {
  def onFailure(e: Throwable): Unit
}
