package org.apache.spark.resource

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import java.nio.file.{Files, Paths}
import java.util.Optional
import scala.util.control.NonFatal

@DeveloperApi
class ResourceID(val componentName: String, val resourceName: String) {
  private [spark] def confPrefix: String = {
    s"$componentName.${ResourceUtils.RESOURCE_PREFIX}.$resourceName"
  }
  private [spark] def amountConf: String = s"$confPrefix${ResourceUtils.AMOUNT}"
  private [spark] def discoveryScriptConf: String = s"$confPrefix${ResourceUtils.DISCOVERY_SCRIPT}"
  private [spark] def vendorConf: String = s"$confPrefix${ResourceUtils.VENDOR}"

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceID =>
        that.getClass == this.getClass &&
        that.componentName == componentName && that.resourceName==resourceName
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(componentName,resourceName).hashCode()
}

@DeveloperApi
class ResourceRequest(val id: ResourceID,
                      val amount: Long,
                      val discoveryScript: Optional[String],
                      val vendor: Optional[String]) {
  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceRequest =>
        that.getClass == this.getClass && that.id == id &&
        that.amount == amount && that.discoveryScript == discoveryScript &&
        that.vendor == vendor
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(id,amount,discoveryScript,vendor).hashCode()
}

private [spark] case class ResourceRequirement(resourceName: String,
                                               amount: Int,
                                               numParts: Int = 1)
private [spark] case class ResourceAllocation(id: ResourceID,address: Seq[String]){
  def toResourceInformation: ResourceInformation = {
    new ResourceInformation(id.resourceName,address.toArray)
  }
}


class ResourceUtils {

}

private [spark] object ResourceUtils extends Logging {
  val DISCOVERY_SCRIPT = "discoveryScript"
  val VENDOR = "vendor"
  val AMOUNT = "amount"
  final val GPU: String = "gpu"
  final val FPGA: String = "fpga"
  final val RESOURCE_PREFIX: String = "resource"
  def parseResourceRequest(sparkConf: SparkConf, resourceId: ResourceID): ResourceRequest = {
    val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
    val amount = settings.getOrElse(AMOUNT,
      throw new SparkException(s"You must specify an amount for ${resourceId.resourceName}")).toInt
    val discoveryScript = Optional.ofNullable(settings.get(DISCOVERY_SCRIPT).orNull)
    val vendor = Optional.ofNullable(settings.get(VENDOR).orNull)
    new ResourceRequest(resourceId,amount,discoveryScript,vendor)
  }
  def listResourceIds(sparkConf: SparkConf, componentName: String): Seq[ResourceID] = {
    sparkConf.getAllWithPrefix(s"$componentName.$RESOURCE_PREFIX").map{ case (key,_) =>
      val index = key.indexOf('.')
      if(index<0) {
        throw new SparkException(s"You must specify an amount config for resource: $key " +
          s"config: $componentName.$RESOURCE_PREFIX.$key")
      }
      key.substring(0,index)
    }.distinct.map(name => new ResourceID(componentName,name))
  }
  def parseAllResourceRequests(sparkConf: SparkConf,
                               componentName: String): Seq[ResourceRequest] = {
    listResourceIds(sparkConf, componentName)
      .map(id => parseResourceRequest(sparkConf,id))
      .filter(_.amount > 0)
  }
  def calculateAmountAndPartsForFraction(doubleAmount: Double): (Int,Int) = {
    val parts = if (doubleAmount <= 0.5) {
      Math.floor(1.0 / doubleAmount).toInt
    }else if(doubleAmount % 1 != 0){
      throw new SparkException(s"The resource amount ${doubleAmount} must be either <= 0.5, or a whole number.")
    }else{
      1
    }
    (Math.ceil(doubleAmount).toInt, parts)
  }
  def addTaskResourceRequests(sparkConf: SparkConf,treqs: TaskResourceRequests): Unit = {
    listResourceIds(sparkConf,SPARK_TASK_PREFIX).map{ resourceId =>
      val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
      val amountDouble = settings.getOrElse(AMOUNT,
        throw new SparkException(s"You must specify an amount for ${resourceId.resourceName}"))
        .toDouble
      treqs.resource(resourceId.resourceName,amountDouble)
    }
  }
  def parseResourceRequirements(sparkConf: SparkConf, componentName: String): Seq[ResourceRequirement] = {
    val resourceIds = listResourceIds(sparkConf,componentName)
    val rnamesAndAmounts = resourceIds.map{resourceId =>
      val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
      val amountDouble = settings.getOrElse(AMOUNT,
        throw new SparkException(s"You must specify an amount for ${resourceId.resourceName}"))
        .toDouble
      (resourceId.resourceName,amountDouble)
    }
    rnamesAndAmounts.filter{ case(_,amount) => amount>0}.map{case(rName,amountDouble) =>
      val (amount,parts) = if (componentName.equalsIgnoreCase(SPARK_TASK_PREFIX)) {
        calculateAmountAndPartsForFraction(amountDouble)
      }else if(amountDouble % 1 !=0 ){
        throw new SparkException(s"Only task support fractional resource, please check your $componentName settings.")
      }else{
        (amountDouble.toInt,1)
      }
      ResourceRequirement(rName,amount,parts)
    }
  }
  def executorResourceRequestToRequirement(resourceRequest: Seq[ExecutorResourceRequest]): Seq[ResourceRequirement] = {
    resourceRequest.map(request =>
      ResourceRequirement(request.resourceName,request.amount.toInt,1))
  }
  def resourceMeetRequirements(resourcesFree: Map[String,Int],
                               resourceRequirements: Seq[ResourceRequirement]): Boolean = {
    resourceRequirements.forall{req =>
      resourcesFree.getOrElse(req.resourceName, 0) >= req.amount
    }
  }
  def withResourcesJson[T](resourcesFile: String)(extract: String => Seq[T]): Seq[T] = {
    val json = new String(Files.readAllBytes(Paths.get(resourcesFile)))
    try{
      extract(json)
    }catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing resources file $resourcesFile", e)
    }
  }
  def parseAllocatedFromJsonFile(resourceFile: String): Seq[ResourceAllocation] = {
    withResourcesJson[ResourceAllocation](resourceFile){
      json =>
        implicit val formats = DefaultFormats
        parse(json).extract[Seq[ResourceAllocation]]
    }
  }
  def parseAllocated(resourceFileOpt: Option[String],
                     componentName: String): Seq[ResourceAllocation] = {
    resourceFileOpt.toSeq.flatMap(parseAllocatedFromJsonFile)
      .filter(_.id.componentName == componentName)
  }
  private def parseAllocatedOrDiscoverResources(sparkConf: SparkConf,
                                                 componentName: String,
                                                 resourcesFileOpt: Option[String]): Seq[ResourceAllocation] = {
    val allocated = parseAllocated(resourcesFileOpt,componentName)
    val otherResourceIds = listResourceIds(sparkConf, componentName).diff(allocated.map(_.id))
    val otherResources = otherResourceIds.flatMap{ id =>
      val request = parseResourceRequest(sparkConf,id)
      if(request.amount > 0){
        Some(ResourceAllocation(id,discoverResource(sparkConf,request).addresses))
      }else{
        None
      }
    }
    allocated ++ otherResources
  }
  private def assertResourceAllocationMeetsRequest(allocation: ResourceAllocation,
                                                   request: ResourceRequest): Unit = {
    require(allocation.id == request.id && allocation.address.size >= request.amount,
      s"Resource: ${allocation.id.resourceName}, with addresses: ${allocation.address.mkString(",")} " +
        s"is less than what the user requested: ${request.amount}")
  }
  private def assertAllResourceAllocationsMeetRequests(allocations: Seq[ResourceAllocation],
                                                       requests: Seq[ResourceRequest]): Unit = {
    val allocated = allocations.map(x => x.id -> x).toMap
    requests.foreach(r => assertResourceAllocationMeetsRequest(allocated(r.id), r))
  }
  private def assertAllResourceAllocationsMatchResourceProfile(allocations: Map[String,ResourceInformation],
                                                               execReqs: Map[String,ExecutorResourceRequest]): Unit = {
    execReqs.foreach{ case(rName,req) =>
      require(allocations.contains(rName) && allocations(rName).addresses.size >= req.amount,
        s"Resource: ${rName}, with addresses: ${allocations(rName).addresses.mkString(",")} " +
          s"is less than what the user requested: ${req.amount}")
    }
  }
  def getOrDiscoverAllResources(sparkConf: SparkConf,
                               componentName: String,
                               resourcesFileOpt: Option[String]): Map[String,ResourceInformation] = {
    val requests = parseAllResourceRequests(sparkConf, componentName)
    val allocations = parseAllocatedOrDiscoverResources(sparkConf, componentName, resourcesFileOpt)
    assertAllResourceAllocationsMeetRequests(allocations, requests)
    val resourceInfoMap = allocations.map(a => (a.id.resourceName, a.toResourceInformation)).toMap
    resourceInfoMap
  }
  private def emptyStringToOptional(optStr: String): Optional[String] = {
    if(optStr.isEmpty){
      Optional.empty[String]
    }else{
      Optional.of(optStr)
    }
  }

}
