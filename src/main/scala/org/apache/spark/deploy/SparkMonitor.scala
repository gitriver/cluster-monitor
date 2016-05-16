package org.apache.spark.deploy

import org.apache.spark.rpc.RpcAddress
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.SparkConf
import org.apache.spark.SecurityManager
import org.apache.spark.deploy.master.WorkerInfo
import org.apache.spark.deploy.DeployMessages.RequestMasterState
import org.apache.spark.deploy.DeployMessages.MasterStateResponse

object SparkMonitor {
  /**
   *
   */
  def getResource(ip: String = "localhost",
    hostName: String = "localhost",
    port: Int = 18989,
    url: String = "spark://bigdata01:7077"): MasterStateResponse = {
    val conf = new SparkConf
    val securityMgr = new SecurityManager(conf)
    val masterAddress = RpcAddress.fromSparkURL(url)
    val rpcEnv = RpcEnv.create(hostName, ip, port, conf, securityMgr)
    val masterEndpoint =
      rpcEnv.setupEndpointRef("sparkMaster", masterAddress, "Master")

    val res: MasterStateResponse = masterEndpoint.askWithRetry[MasterStateResponse](RequestMasterState)
    res
  }

  def main(args: Array[String]): Unit = {
    val res: MasterStateResponse = getResource("10.201.26.29")
    //打印spark worker进程信息
    res.workers.foreach { worker =>
      println(
        worker.id,
        worker.host,
        worker.state,
        worker.cores,
        worker.coresUsed,
        worker.memory,
        worker.memoryUsed)
    }
  }
}