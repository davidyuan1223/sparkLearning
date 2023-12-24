package org.apache.spark
import java.time.DateTimeException
import java.util
import java.util.ConcurrentModificationException
import scala.collection.JavaConverters.mapAsJavaMapConverter

class SparkException(message: String, cause: Throwable,
                     errorClass: Option[String], messageParameters: Map[String,String],
                     context: Array[QueryContext] = Array.empty)extends Exception(message,cause) with SparkThrowable{
  def this(message: String, cause: Throwable) =
    this(message=message,cause=cause,errorClass=None,messageParameters=Map.empty)

  def this(message: String) =
    this(message=message,cause=null)

  def this(errorClass: String,messageParameters: Map[String,String],
           cause: Throwable, context: Array[QueryContext],summary: String) =
    this(message=SparkThrowableHelper.getMessage(errorClass, messageParameters,summary),
      cause=cause,errorClass=Some(errorClass),messageParameters=messageParameters,context)

  def this(errorClass: String, messageParameters: Map[String,String], cause: Throwable) =
    this(message=SparkThrowableHelper.getMessage(errorClass, messageParameters),
      cause=cause,errorClass=Some(errorClass),messageParameters=messageParameters)

  override def getMessageParameters: util.Map[String, String] = messageParameters.asJava

  override def getErrorClass: String = errorClass.orNull

  override def getQueryContext: Array[QueryContext] = context
}

object SparkException{
  def internalError(msg: String, context: Array[QueryContext], summary: String): SparkException = {
    new SparkException(
      errorClass = "INTERNAL_ERROR",
      messageParameters = Map("message" -> msg),
      cause = null,
      context,
      summary)
  }

  def internalError(msg: String): SparkException ={
    internalError(msg,context = Array.empty[QueryContext],summary = "")
  }

  def internalError(msg: String, cause: Throwable): SparkException = {
    new SparkException(
      errorClass ="INTERNAL_ERROR",
      messageParameters = Map("message" -> msg),
      cause = cause
    )
  }
}

private [spark] class SparkDriverExecutionException(cause: Throwable) extends SparkException("Execution error",cause)
private [spark] case class SparkUserAppException(exitCode: Int) extends SparkException(s"User application exited with $exitCode")
private [spark] case class ExecutorDeadException(message: String) extends SparkException(message)
private [spark] class SparkUpgradeException(errorClass: String,messageParameters: Map[String,String],cause: Throwable)
  extends RuntimeException(SparkThrowableHelper.getMessage(errorClass, messageParameters),cause) with SparkThrowable{
  override def getMessageParameters: util.Map[String, String] = messageParameters.asJava
  override def getErrorClass: String = errorClass
}
private [spark] class SparkUnsupportedOperationException(errorClass: String,messageParameters: Map[String,String])
  extends UnsupportedOperationException(SparkThrowableHelper.getMessage(errorClass, messageParameters)) with SparkThrowable{
  override def getMessageParameters: util.Map[String, String] = messageParameters.asJava
  override def getErrorClass: String = errorClass
}

private [spark] class SparkClassNotFoundException(errorClass: String,messageParameters: Map[String,String],
                                                         cause: Throwable=null)
  extends ClassNotFoundException(SparkThrowableHelper.getMessage(errorClass, messageParameters),cause) with SparkThrowable{
  override def getMessageParameters: util.Map[String, String] = messageParameters.asJava
  override def getErrorClass: String = errorClass
}

private [spark] class SparkConcurrentModificationException(errorClass: String,messageParameters: Map[String,String],
                                                           cause: Throwable=null)
  extends ConcurrentModificationException(SparkThrowableHelper.getMessage(errorClass, messageParameters),cause) with SparkThrowable{
  override def getMessageParameters: util.Map[String, String] = messageParameters.asJava
  override def getErrorClass: String = errorClass
}

private [spark] class SparkDateTimeException(errorClass: String,messageParameters: Map[String,String],
                                             context: Array[QueryContext],summary: String)
  extends DateTimeException(SparkThrowableHelper.getMessage(errorClass, messageParameters,summary)) with SparkThrowable{
  override def getMessageParameters: util.Map[String, String] = messageParameters.asJava
  override def getErrorClass: String = errorClass
  override def getQueryContext: Array[QueryContext] = context
}

private [spark] class SparkFileAlreadyExistException(errorClass: String,messageParameters: Map[String,String])
  extends FileAlreadyExistException(SparkThrowableHelper.getMessage(errorClass, messageParameters)) with SparkThrowable{
  override def getMessageParameters: util.Map[String, String] = messageParameters.asJava
  override def getErrorClass: String = errorClass
}
