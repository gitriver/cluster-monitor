package org.apache.spark.deploy

import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.SparkConf
import org.apache.spark.SecurityManager
import org.apache.spark.deploy.master.WorkerInfo

object SparkMonitor {
  def getResource(ip: String = "192.168.1.100", port: Int = 18989, hostName: String = "localhost", url: String = "spark://bigdata01:7077"): Array[WorkerInfo] = {
    val conf = new SparkConf
    val securityMgr = new SecurityManager(conf)
    val masterAddress = RpcAddress.fromSparkURL(url)
    val rpcEnv = RpcEnv.create(hostName, ip, port, conf, securityMgr)
    val masterEndpoint =
      rpcEnv.setupEndpointRef("sparkMaster", masterAddress, "Master")

    val res = masterEndpoint.askWithRetry[MasterStateResponse](RequestMasterState)
    res.workers
  }

  def main(args: Array[String]): Unit = {
    val res = getResource("192.168.1.100")
    res.foreach { worker => println(worker.id, worker.host, worker.state, worker.cores, worker.coresUsed, worker.memory, worker.memoryUsed) }
  }
}