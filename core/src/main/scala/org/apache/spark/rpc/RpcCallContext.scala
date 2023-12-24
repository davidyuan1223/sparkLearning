package org.apache.spark.rpc

private [spark] trait RpcCallContext {
  def reply(response: Any): Unit
  def sendFailure(e: Throwable): Unit
  def  senderAddress: RpcAddress
}
