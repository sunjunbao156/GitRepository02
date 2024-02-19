package top.doe.spark.util

case class RpcAddress(host: String, port: Int) {

  def hostPort: String = host + ":" + port

  override def toString: String = hostPort
}
