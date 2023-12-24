package org.apache.spark

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.text.StringSubstitutor
import org.apache.spark.annotation.DeveloperApi

import java.net.URL
import scala.collection.JavaConverters.mapAsJavaMapConverter

@DeveloperApi
class ErrorClassesJsonReader(jsonFileURLs: Seq[URL]) {
  assert(jsonFileURLs.nonEmpty)
  private [spark] val errorInfoMap =
    jsonFileURLs.map(ErrorClassesJsonReader.readAsMap).reduce(_ ++ _)

  def getErrorMessage(errorClass: String, messageParameters: Map[String,String]): String = {
    val messageTemplate = getMessageTemplate(errorClass)
    val sub = new StringSubstitutor(messageParameters.asJava)
    sub.setEnableUndefinedVariableException(true)
    sub.setDisableSubstitutionInValues(true)
    try{
      sub.replace(messageTemplate.replaceAll("<([a-zA-Z0-9_-]+)>","\\$\\{$1\\"))
    }catch {
      case _: IllegalArgumentException => throw SparkException.internalError(
        s"Undefined error message parameter for error class: $errorClass. Parameters: $messageParameters"
      )
    }
  }

  def getMessageTemplate(errorClass: String): String = {
    val errorClasses = errorClass.split("\\.")
    assert(errorClasses.length == 1 || errorClasses.length == 2)
    val mainErrorClass = errorClasses.head
    val subErrorClass = errorClasses.tail.headOption
    val errorInfo = errorInfoMap.getOrElse(mainErrorClass,
      throw SparkException.internalError(s"Cannot find main error class '$errorClass'"))
    assert(errorInfo.subClass.isDefined == subErrorClass.isDefined)
    if (subErrorClass.isEmpty){
      errorInfo.messageTemplate
    }else{
      val errorSubInfo = errorInfo.subClass.get.getOrElse(
        subErrorClass.get,
        throw SparkException.internalError(s"Cannot find sub error class '$errorClass'")
      )
      errorInfo.messageTemplate+" "+errorSubInfo.messageTemplate
    }
  }

  def getSqlState(errorClass: String): String = {
    Option(errorClass)
      .flatMap(_.split(".").headOption)
      .flatMap(errorInfoMap.get)
      .flatMap(_.sqlState)
      .orNull
  }
}

private object ErrorClassesJsonReader{
  private val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()
  private def readAsMap(url: URL): Map[String,ErrorInfo] = {
    val map = mapper.readValue(url, new TypeReference[Map[String,ErrorInfo]]() {})
    val errorClassWithDots = map.collectFirst{
      case (errorClass, _) if errorClass.contains(".") => errorClass
      case (_, ErrorInfo(_,Some(map),_)) if map.keys.exists(_.contains(".")) =>
        map.keys.collectFirst{
          case s if s.contains(".") => s
        }.get
    }
    if (errorClassWithDots.isEmpty){
      map
    }else {
      throw SparkException.internalError(
        s"Found the (sub-)error class with dots: ${errorClassWithDots.get}"
      )
    }
  }
}

private case class ErrorInfo(message: Seq[String],
                             subClass: Option[Map[String,ErrorSubInfo]],
                             sqlState: Option[String]) {
  @JsonIgnore
  val messageTemplate: String = message.mkString("\n")
}

private case class ErrorSubInfo(message: Seq[String]) {
  @JsonIgnore
  val messageTemplate: String = message.mkString("\n")
}
