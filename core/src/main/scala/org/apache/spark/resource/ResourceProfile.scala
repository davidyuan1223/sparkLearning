package org.apache.spark.resource

import org.apache.spark.SparkConf
import org.apache.spark.annotation.{Evolving, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy


@Evolving
@Since("3.1.0")
class ResourceProfile(
                     val executorResources: Map[String,ExecutorResourceRequest],
                     val taskResources: Map[String,TaskResourceRequest]
                     )extends Serializable  with Logging{

}

object ResourceProfile extends Logging {
  val CPUS = "cpus"
  val CORES = "cores"
  val MEMORY = "memory"
  val OFFHEAP_MEM = "offHeap"
  val OVERHEAD_MEM = "memoryOverhead"
  val PYSPARK_MEM = "pyspark.memory"
  def allSupportedExecutorResources: Array[String] =
    Array(CORES,MEMORY,OVERHEAD_MEM,PYSPARK_MEM,OFFHEAP_MEM)
  val UNKNOWN_RESOURCE_PROFILE_ID = -1
  val DEFAULT_RESOURCE_PROFILE_ID = 0
  private [spark] val MEMORY_OVERHEAD_MIN_MIB = 384L
  private lazy val nextProfileId = new AtomicInteger(0)
  private val DEFAULT_PROFILE_LOCK = new Object
  @GuardedBy("DEFAULT_PROFILE_LOCK")
  private var defaultProfile: Option[ResourceProfile] = None
  private var defaultProfileExecutorResources: Option[DefaultProfileExecutorResources] = None
  private [spark] def getOrCreateDefaultProfile(conf: SparkConf): ResourceProfile = {
    DEFAULT_PROFILE_LOCK.synchronized{
      defaultProfile match {
        case Some(prof) => prof
        case None =>
          val taskResources = getDefaultTaskResources(conf)
          val executorResources = getDefaultExecutorResources(conf)
          val defProf = new ResourceProfile(executorResources,taskResources)
          defProf.setToDefaultProfile()
          logInfo("Default ResourceProfile created, executor resources: " +
            s"${defProf.executorResources}, task resources: ${defProf.taskResources}")
          defProf
      }
    }
  }
  private [spark] def getDefaultProfileExecutorResources(conf: SparkConf): DefaultProfileExecutorResources = {
    defaultProfileExecutorResources.getOrElse{
      getOrCreateDefaultProfile(conf)
      defaultProfileExecutorResources.get
    }
  }
  private def getDefaultTaskResources(conf: SparkConf): Map[String,TaskResourceRequest] = {
    val cpusPerTask = conf.get(CPUS_PER_TASK)
    val treqs = new TaskResourceRequests().cpus(cpusPerTask)
    ResourceUtils
  }


  private [spark] case class ExecutorResourceOrDefaults(cores: Option[Int],
                                                        executorMemoryMiB: Long,
                                                        memoryOfHeapMiB: Long,
                                                        pySparkMemoryMiB: Long,
                                                        memoryOverheadMiB: Long,
                                                        totalMemMiB: Long,
                                                        customResources: Map[String, ExecutorResourceRequest])

  private [spark] case class DefaultProfileExecutorResources(cores: Option[Int],
                                                             executorMemoryMiB: Long,
                                                             memoryOffHeapMiB: Long,
                                                             pysparkMemoryMiB: Option[Long],
                                                             memoryOverheadMiB: Option[Long],
                                                             customResources: Map[String,ExecutorResourceRequest])
  private [spark] def calculateOverHeapMemory(
                                             overHeapMemFromConf: Option[Long],
                                             executorMemoryMiB: Long,
                                             overheadFactor: Double
                                             ): Long = {
    overHeapMemFromConf.getOrElse(math.max((overheadFactor*executorMemoryMiB).toInt,
      ResourceProfile.MEMORY_OVERHEAD_MIN_MIB))
  }
}
