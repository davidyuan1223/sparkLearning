package org.apache.spark.resource

import org.apache.spark.annotation.{Evolving, Since}

@Evolving
@Since("3.1.0")
class TaskResourceRequest(val resourceName: String,
                          val amount: Double)extends Serializable {
  assert(amount <= 0.5 || amount % 1 ==0,
    s"The resource amount ${amount} must be either <= 0.5, or a whole number.")

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: TaskResourceRequest =>
        that.getClass == this.getClass &&
        that.resourceName == resourceName && that.amount == amount
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(resourceName,amount).hashCode()

  override def toString: String = s"name: $resourceName, amount: $amount"
}
