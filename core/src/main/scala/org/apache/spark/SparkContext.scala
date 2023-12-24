package org.apache.spark

import org.apache.hadoop.io.{ArrayWritable, Writable}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{CallSite, Utils}
import org.apache.spark.internal.config._

import java.util.concurrent.atomic.AtomicReference
import scala.reflect.{ClassTag, classTag}
class SparkContext(conf: SparkConf) extends Logging {
  private val creationSite: CallSite = Utils.getCallSite()
  if(!conf.get(EXECUTOR_ALLOW_SPARK_CONTEXT)){
    SparkContext.assertOnDriver()
  }
}


object SparkContext extends Logging {
  private val VALID_LOG_LEVELS =
    Set("ALL","DEBUG","ERROR","FATAL","INFO","OFF","TRACE","WARN")
  private val SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object()
  private val activeContext: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null)
  private var contextBeingConstructed: Option[SparkContext] = None
  private def assertNoOtherContextIsRunning(sc: SparkContext): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized{
      Option(activeContext.get()).filter(_ ne sc).foreach{ctx =>
        val errMsg = "Only one sparkContext should be running in this JVM (see SPARK-2243)." +
          s"The currently running SparkContext was created at:\n${ctx.creationSite.longForm}"
        throw new SparkException(errMsg)
      }
      contextBeingConstructed.filter(_ ne sc).foreach{otherContext =>
        val otherContextCreationSite=
          Option(otherContext.creationSite).map(_.longForm).getOrElse("unknown location")
        val warnMsg="Another SparkContext is being constructed (or threw an exception in its constructor)." +
          "This may indicate an error, since only one SparkContext should be running in the JVM (see SPARK-2243)." +
          s"The other SparkContext was created at:\n$otherContextCreationSite"
        logWarning(warnMsg)
      }
    }
  }
  private def assertOnDriver(): Unit = {
    if(Utils.isInRunningSparkTasks){
      throw new IllegalStateException("SparkContext ohould only be created and accessed on the driver.")
    }
  }
  def getOrCreate(conf: SparkConf): SparkContext = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized{
      if(activeContext.get()==null){
        setActiveContext(new SparkContext(conf))
      }else{
        if(conf.getAll.nonEmpty) {
          logWarning("Using an existing SparkContext; some configuration may not take effect.")
        }
      }
      activeContext.get()
    }
  }
  def getOrCreate(): SparkContext = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized{
      if(activeContext.get()==null){
        setActiveContext(new SparkContext())
      }
      activeContext.get()
    }
  }
  private [spark] def getActive: Option[SparkContext] = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized{
      Option(activeContext.get())
    }
  }
  private [spark] def markPartiallyConstructed(sc: SparkContext): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized{
      assertNoOtherContextIsRunning(sc)
      contextBeingConstructed=Some(sc)
    }
  }
  private [spark] def setActiveContext(sc: SparkContext): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized{
      assertNoOtherContextIsRunning(sc)
      contextBeingConstructed=None
      activeContext.set(sc)
    }
  }
  private [spark] def clearActiveContext(): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized{
      activeContext.set(null)
    }
  }
  private [spark] val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private [spark] val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  private [spark] val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"
  private [spark] val SPARK_SCHEDULER_POOL = "spark.scheduler.pool"
  private [spark] val RDD_SCOPE_KEY = "spark.rdd.scope"
  private [spark] val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"
  private [spark] val DRIVER_IDENTIFIER = "driver"
  private implicit def arrayToArrayWritable[T <: Writable: ClassTag](arr: Iterable[T]): ArrayWritable = {
    def anyToWritable[U <: Writable](u: U): Writable = u
    new ArrayWritable(classTag[T].runtimeClass.asInstanceOf[Class[Writable]],
      arr.map(x => anyToWritable(x)).toArray)
  }
  def jarOfClass(cls: Class[_]): Option[String] = {
    val uri = cls.getResource("/"+cls.getName.replace('.','/')+".class")
    if(uri!=null){
      val uriStr = uri.toString
      if(uriStr.startsWith("jar:file:")){
        Some(uriStr.substring("jar:File:".length,uriStr.indexOf('!')))
      }else{
        None
      }
    }else{
      None
    }
  }
  def jarOfObject(obj: AnyRef): Option[String] = jarOfClass(obj.getClass)
  private [spark] def updateConf(
                                conf: SparkConf,
                                master: String,
                                appName: String,
                                sparkHome: String=null,
                                jars: Seq[String]=Nil,
                                environment: Map[String,String]=Map()
                                ):SparkConf = {
    val res = conf.clone
    res.setMaster(master)
    res.setAppName(appName)
    if (sparkHome!=null){
      res.setSparkHome(sparkHome)
    }
    if(jars!=null && !jars.isEmpty){
      res.setJars(jars)
    }
    res.setExecutorEnv(environment.toSeq)
    res
  }
  private [spark] def numDriverCores(master: String): Int = {
    numDriverCores(master,null)
  }
  private [spark] def numDriverCores(master: String, conf: SparkConf): Int = {
    def convertToInt(threads: String): Int = {
      if(threads=="*") Runtime.getRuntime.availableProcessors() else threads.toInt
    }
    master match {
      case "local" => 1
      case SparkMasterRegex.LOCAL_N_REGEX(threads) => convertToInt(threads)
      case SparkMasterRegex.LOCAL_N_FAILURES_REGEX(threads, _) => convertToInt(threads)
      case "yarn" | SparkMasterRegex.KUBERNETES_REGEX(_) =>
        if(conf != null && conf.get(SUBMIT_DEPLOY_MODE) == "cluster"){
          conf.getInt(DRIVER_CORES.key,0)
        }else{
          0
        }
      case _ => 0
    }
  }
  private [spark] def executorMemoryInMb(conf: SparkConf): Int = {
    conf.getOption(EXECUTOR_MEMORY.key)
      .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
      .orElse(Option(System.getenv("SPARK_MEM")))
      .map(warnSparkMem)
      .map(Utils.memoryStringToMb)
      .getOrElse(1024)
  }
  private def warnSparkMem(value: String): String = {
    logWarning("Using SPARK_MEM to set amount of memory to use per executor process is" +
      "deprecated, please use spark.executor.memory instead")
    value
  }
  private def createTaskScheduler(sc: SparkContext,
                                  master: String): (SchedulerBackend,TaskScheduler) = {

  }
}
