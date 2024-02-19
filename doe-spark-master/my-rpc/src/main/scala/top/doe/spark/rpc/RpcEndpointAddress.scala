package top.doe.spark.rpc

import top.doe.spark.util.RpcAddress

case class RpcEndpointAddress(rpcAddress: RpcAddress, name: String) {

  def this(host: String, port: Int, name: String) = {
    this(RpcAddress(host, port), name)
  }

  override def toString: String = if(rpcAddress != null) {
    //远端的RpcEndpoint地址
    s"spark://$name@${rpcAddress.host}:${rpcAddress.port}"
  } else {
    //本地的RpcEndpoint地址
    s"spark-client://$name"
  }
}

object RpcEndpointAddress {

  //根据spark的url，返回RpcEndpointAddress，例如spark://Master@localhost:8888
  def apply(sparkUrl: String): RpcEndpointAddress = {
    try {
      val uri = new java.net.URI(sparkUrl)
      val host = uri.getHost
      val port = uri.getPort
      val name = uri.getUserInfo
      if (uri.getScheme != "spark" ||
        host == null ||
        port < 0 ||
        name == null ||
        (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null) {
        throw new Exception("Invalid Spark URL: " + sparkUrl)
      }
      new RpcEndpointAddress(host, port, name)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new Exception("Invalid Spark URL: " + sparkUrl, e)
    }
  }
}

