package top.doe.spark.deploy

import top.doe.spark.rpc.RpcEndpointRef

//Worker发送给Master注册的消息
case class RegisterWorker(rpcEndpointRef: RpcEndpointRef, workerId: String, workerMemory: Long, workerCores: Long)
