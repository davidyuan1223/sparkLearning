package org.apache.spark.resource

import org.apache.spark.annotation.{Evolving, Since}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaConcurrentMapConverter}
import java.util.{Map => JMap}
import org.apache.spark.resource.ResourceProfile._
@Evolving
@Since("3.1.0")
class TaskResourceRequests extends Serializable {
  private val _taskResources = new ConcurrentHashMap[String,TaskResourceRequest]()
  def requests: Map[String,TaskResourceRequest] = _taskResources.asScala.toMap
  def requestsJMap: JMap[String,TaskResourceRequest] = requests.asJava
  def cpus(amount: Int): this.type = {
    val treq = new TaskResourceRequest(CPUS, amount)
    _taskResources.put(CPUS,treq)
    this
  }
  def resource(resourceName: String, amount: Double): this.type  = {
    val treq = new TaskResourceRequest(resourceName,amount)
    _taskResources.put(resourceName,treq)
    this
  }
  def addRequest(treq: TaskResourceRequest): this.type  = {
    _taskResources.put(treq.resourceName,treq)
    this
  }

  override def toString: String = s"Task resource requests: ${_taskResources}"
}


