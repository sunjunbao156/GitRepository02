package top.doe.spark.netty

import top.doe.spark.rpc.{RpcEnv, RpcEnvConfig, RpcEnvFactory}
import top.doe.spark.serializer.{JavaSerializer, JavaSerializerInstance}

class NettyRpcEnvFactory extends RpcEnvFactory{

  override def create(config: RpcEnvConfig): RpcEnv = {
    //在NettyRpcEnvFactory的create方法中，先创建序列化器
    val javaSerializerInstance = new JavaSerializer().newInstance().asInstanceOf[JavaSerializerInstance]
    //将创建好的序列化器传入到NettyRpcEnv中
    val nettyEnv = new NettyRpcEnv(config.numUsableCores, javaSerializerInstance)
    nettyEnv.startServer(config.bindAddress, config.port)
    nettyEnv
  }
}
