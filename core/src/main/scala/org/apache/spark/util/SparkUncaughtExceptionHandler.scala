package org.apache.spark.util

import org.apache.spark.internal.Logging

private [spark] class SparkUncaughtExceptionHandler(val exitOnUncaughtException: Boolean = true)
  extends Thread.UncaughtExceptionHandler with Logging{
  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    try {
      val inShutdownMsg = if (ShutdownHookManager.inShutdow()) "[Container in shutdown] " else ""
      val errMsg = "Uncaught exception in thread "
      logError(inShutdownMsg+errMsg+t,e)
      if (!ShutdownHookManager.inShutdow()) {
        e match {
          case _ : OutOfMemoryError =>
            System.exit(SparkExitCode.OOM)
          case ex: SparkFatalException if ex.throwable.isInstanceOf[OutOfMemoryError] =>
            System.exit(SparkExitCode.OOM)
          case _ if exitOnUncaughtException =>
            System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
          case _ =>
        }
      }
    }catch {
      case oom: OutOfMemoryError =>
        try {
          logError(s"Uncaught OutOfMemoryError in thread $t, process halted.",oom)
        }catch {
          case _: Throwable =>
        }
        Runtime.getRuntime.halt(SparkExitCode.OOM)
      case th: Throwable =>
        try {
          logError(s"Another uncaught exception in thread $t, process halted.",th)
        }catch {
          case _: Throwable =>
        }
        Runtime.getRuntime.halt(SparkExitCode.UNCAUGHT_EXCEPTION_TWICE)
    }
  }

  def uncaughtException(exception: Throwable): Unit = {
    uncaughtException(Thread.currentThread(),exception)
  }
}
