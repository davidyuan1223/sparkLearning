package org.apache.spark

import org.apache.spark.util.JsonProtocol.toJsonString
import org.apache.spark.util.Utils

import scala.collection.JavaConverters.mapAsScalaMapConverter

private [spark] object ErrorMessageFormat extends Enumeration {
  val PRETTY, MINIMAL, STANDARD = Value
}
private [spark] object SparkThrowableHelper {
  val errorReader = new ErrorClassesJsonReader(
    Seq(Utils.getSparkClassLoader.getResource("error/error-classes.json"))
  )
  def getMessage(errorClass: String,
                 messageParameters: Map[String,String]): String = {
    getMessage(errorClass, messageParameters,"")
  }
  def getMessage(errorClass: String,
                 messageParameters: java.util.Map[String,String]): String = {
    getMessage(errorClass, messageParameters.asScala.toMap,"")
  }
  def getMessage(errorClass: String,
                 messageParameters: Map[String,String],
                 context: String): String = {
    val displayMessage=errorReader.getErrorMessage(errorClass, messageParameters)
    val displayQueryContext=(if (context.isEmpty) "" else "\n") +context
    val prefix = if(errorClass.startsWith("_LEGACY_ERROR_TEMP_")) "" else s"[$errorClass] "
    s"$prefix$displayMessage$displayQueryContext"
  }
  def getSqlState(errorClass: String): String = {
    errorReader.getSqlState(errorClass)
  }
  def getMessage(e: SparkThrowable with Throwable, format: ErrorMessageFormat.Value): String = {
    import ErrorMessageFormat._
    format match {
      case PRETTY => e.getMessage
      case MINIMAL | STANDARD if e.getErrorClass == null =>
        toJsonString{
          generator =>
            val g = generator.useDefaultPrettyPrinter()
            g.writeStartObject()
            g.writeStringField("errorClass","LEGACY")
            g.writeObjectFieldStart("messageParameters")
            g.writeStringField("message",e.getMessage)
            g.writeEndObject()
            g.writeEndObject()
        }
      case MINIMAL | STANDARD =>
        val errorClass = e.getErrorClass
        toJsonString{generator =>
          val g=generator.useDefaultPrettyPrinter()
          g.writeStartObject()
          g.writeStringField("errorClass",errorClass)
          if (format==STANDARD) {
            g.writeStringField("messageTemplate", errorReader.getMessageTemplate(errorClass))
          }
          val sqlState = e.getSqlState
          if (sqlState != null) g.writeStringField("sqlState",sqlState)
          val messageParameters = e.getMessageParameters
          if(!messageParameters.isEmpty){
            g.writeObjectFieldStart("messageParameters")
            messageParameters.asScala
              .toMap
              .toSeq.sortBy(_._1)
              .foreach{ case (name,value) =>
              g.writeStringField(name,value.replaceAll("#\\d+","#x"))}
            g.writeEndObject()
          }
          val queryContext = e.getQueryContext
          if (!queryContext.isEmpty) {
            g.writeArrayFieldStart("queryContext")
            e.getQueryContext.foreach{ c=>
              g.writeStartObject()
              g.writeStringField("objectType",c.objectType())
              g.writeStringField("objectName",c.objectName())
              val startIndex = c.startIndex()+1
              if (startIndex>0) g.writeNumberField("startIndex",startIndex)
              val stopIndex = c.stopIndex()+1
              if (stopIndex>0) g.writeNumberField("stopIndex",stopIndex)
              g.writeStringField("fragment",c.fragment())
              g.writeEndObject()
            }
            g.writeEndArray()
          }
          g.writeEndObject()
        }
    }
  }

  def getMessage(throwable: Throwable): String ={
    toJsonString{generator =>
      val g = generator.useDefaultPrettyPrinter()
      g.writeStartObject()
      g.writeStringField("errorClass", throwable.getClass.getCanonicalName)
      g.writeObjectFieldStart("messageParameters")
      g.writeStringField("message", throwable.getMessage)
      g.writeEndObject()
      g.writeEndObject()
    }
  }
}
