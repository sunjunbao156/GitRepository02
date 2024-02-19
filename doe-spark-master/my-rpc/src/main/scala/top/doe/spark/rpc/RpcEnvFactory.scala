package top.doe.spark.rpc

trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv

}
