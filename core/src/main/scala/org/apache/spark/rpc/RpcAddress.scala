package org.apache.spark.rpc

private [spark] case class RpcAddress(host: String, port: Int) {
  def hostPort: String = host+":"+port
  def toSparkURL: String = "spark://"+hostPort

  override def toString: String = hostPort
}

private [spark] object RpcAddress{
  def apply(host: String, port: Int): RpcAddress = {
    new RpcAddress(Utils.normalizeIpIfNeeded(host)),
    port
  }
}
