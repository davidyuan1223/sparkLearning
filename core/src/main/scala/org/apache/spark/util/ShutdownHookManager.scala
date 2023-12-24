package org.apache.spark.util

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.internal.Logging

import java.io.File
import java.util.PriorityQueue
import scala.collection.mutable
import scala.util.Try

private [spark] object ShutdownHookManager extends Logging{
  val DEFAULT_SHUTDOWN_PRIORITY = 100
  val SPARK_CONTEXT_SHUTDOWN_PRIORITY = 50
  val TEMP_DIR_SHUTDOWN_PRIORITY = 25

  private lazy val shutdownHooks = {
    val manager = new SparkShutdownHookManager()
    manager.install()
    manager
  }

  private val shutdownDeletePaths = new mutable.HashSet[String]()
  logDebug("Adding shutdown hook")
  addShutdownHook(TEMP_DIR_SHUTDOWN_PRIORITY) {
    () =>
      logInfo("Shutdown hook called")
      shutdownDeletePaths.toArray.foreach { dirPath =>
        try{
          logInfo("Deleting directory "+dirPath)
          Utils.deleteRecursively(new File(dirPath))
        }catch {
          case e: Exception => logError(s"Exception while deleting Spark temp dir: $dirPath",e)
        }
      }
  }

  def registerShutdownDeleteDir(file: File): Unit = {
    val absolutePath = file.getAbsolutePath
    shutdownDeletePaths.synchronized{
      shutdownDeletePaths += absolutePath
    }
  }

  def removeShutdownDeleteDir(file: File): Unit = {
    val absolutePath = file.getAbsolutePath
    shutdownDeletePaths.synchronized{
      shutdownDeletePaths.remove(absolutePath)
    }
  }

  def hasShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath
    shutdownDeletePaths.synchronized{
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  def hasRootAsShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath
    val retval = shutdownDeletePaths.synchronized{
      shutdownDeletePaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if(retval) {
      logInfo("path = "+file+", already present as root for deletion.")
    }
    retval
  }

  def inShutdow(): Boolean = {
    try{
      val hook = new Thread{
        override def run(): Unit = {}
      }
      Runtime.getRuntime.addShutdownHook(hook)
      Runtime.getRuntime.removeShutdownHook(hook)
    }catch {
      case ise: IllegalStateException => return true
    }
    false
  }

  def addShutdownHook(hook: () => Unit): AnyRef = {
    addShutdownHook(DEFAULT_SHUTDOWN_PRIORITY)(hook)
  }

  def addShutdownHook(priority: Int)(hook: () => Unit): AnyRef = {
    shutdownHooks.add(priority, hook)
  }

  def removeShutdownHook(ref: AnyRef): Boolean = {
    shutdownHooks.remove(ref)
  }

}


private [util] class SparkShutdownHookManager {
  private val hooks = new PriorityQueue[SparkShutdownHook]
  @volatile private var shuttingDown = false
  def install(): Unit = {
    val hookTask = new Runnable {
      override def run(): Unit = runAll()
    }
    ShutdownHookManager.get().addShutdownHook(
      hookTask, FileSystem.SHUTDOWN_HOOK_PRIORITY+30
    )
  }

  def runAll(): Unit = {
    shuttingDown = true
    var nextHook: SparkShutdownHook = null
    while ({nextHook = hooks.synchronized{ hooks.poll() }; nextHook!=null}) {
      Try(Utils.logUncaughtExceptions(nextHook.run()))
    }
  }

  def add(priority: Int, hook: ()=>Unit): AnyRef = {
    hook.synchronized{
      if(shuttingDown) {
        throw new IllegalStateException("Shutdown hooks cannot be modified during shutdown.")
      }
      val hookRef = new SparkShutdownHook(priority,hook)
      hooks.add(hookRef)
      hookRef
    }
  }

  def remove(ref: AnyRef): Boolean = {
    hooks.synchronized{
      hooks.remove(ref)
    }
  }
}

private class SparkShutdownHook(private val priority: Int, hook: () => Unit)
  extends Comparable[SparkShutdownHook] {
  override def compareTo(o: SparkShutdownHook): Int = o.priority.compareTo(priority)
  def run(): Unit = hook()
}
