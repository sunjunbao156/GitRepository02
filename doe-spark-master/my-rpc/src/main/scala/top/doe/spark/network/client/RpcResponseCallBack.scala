package top.doe.spark.network.client

import java.nio.ByteBuffer

trait RpcResponseCallBack extends BaseResponseCallBack {

  def onSuccess(response: ByteBuffer): Unit

}
