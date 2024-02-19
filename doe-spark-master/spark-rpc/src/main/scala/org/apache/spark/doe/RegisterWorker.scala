package org.apache.spark.doe

import org.apache.spark.rpc.RpcEndpointRef

case class RegisterWorker(rpcEndpointRef: RpcEndpointRef,workerId: String, workerMemory: Int, workerCores: Int)
