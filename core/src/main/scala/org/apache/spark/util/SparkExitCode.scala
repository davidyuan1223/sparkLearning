package org.apache.spark.util

private [spark] object SparkExitCode {
  val EXIT_SUCCESS = 0
  val EXIT_FAILURE = 1
  val ERROR_MISUSE_SHELL_BUILTIN = 2
  val ERROR_PATH_NOT_FOUND = 3
  val UNCAUGHT_EXCEPTION = 50
  val UNCAUGHT_EXCEPTION_TWICE = 51
  val OOM=52
  val ERROR_COMMAND_NOT_FOUND = 127
}
