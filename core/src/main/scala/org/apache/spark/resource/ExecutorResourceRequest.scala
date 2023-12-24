package org.apache.spark.resource

import org.apache.spark.annotation.{Evolving, Since}

@Evolving
@Since("3.1.0")
class ExecutorResourceRequest(
                             val resourceName: String,
                             val amount: Long,
                             val discoveryScript: String="",
                             val vendor: String=""
                             )extends Serializable {
  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ExecutorResourceRequest =>
        that.getClass == this.getClass &&
        that.resourceName == resourceName && that.amount==amount &&
        that.discoveryScript==discoveryScript && that.vendor==vendor
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(resourceName,amount,discoveryScript,vendor).hashCode()

  override def toString: String = {
    s"name: $resourceName, amount: $amount, script: $discoveryScript, vendor: $vendor"
  }
}
