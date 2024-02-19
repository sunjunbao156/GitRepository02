package org.apache.spark.doe

class WorkerInfo(val workerId: String, var workerMemory: Int, var workerCores: Int) {

  var lastHeartBeatTime: Long = _
}
