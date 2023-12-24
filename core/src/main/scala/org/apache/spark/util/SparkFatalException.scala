package org.apache.spark.util

private [spark] final class SparkFatalException(val throwable: Throwable) extends Exception(throwable)
