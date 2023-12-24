package org.apache.spark.resource

import org.apache.spark.SparkException
import org.apache.spark.annotation.Evolving
import org.json4s.{DefaultFormats, Extraction, JValue}
import org.json4s.jackson.JsonMethods._

import scala.util.control.NonFatal

@Evolving
class ResourceInformation(val name: String,
                          val addresses: Array[String]) extends Serializable {
  override def toString: String = s"[name: ${name}, addresses: ${addresses.mkString(",")}]"

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceInformation =>
        that.getClass == this.getClass &&
        that.name==name && that.addresses.toSeq==addresses.toSeq
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(name,addresses.toSeq).hashCode()
  final def toJson(): JValue = ResourceInformationJson(name,addresses).toJValue
}

private [spark] object ResourceInformation {
  private lazy val exampleJson: String = compact(render(
    ResourceInformationJson("gpu",Seq("0","1")).toJValue
  ))
  def parseJson(json: String): ResourceInformation = {
    implicit val formats = DefaultFormats
    try {
      parse(json).extract[ResourceInformationJson].toResourceInformation
    }catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n"+
        s"Here is a correct example: $exampleJson.",e)
    }
  }
  def parsJson(json: JValue): ResourceInformation = {
    implicit val formats = DefaultFormats
    try {
      json.extract[ResourceInformationJson].toResourceInformation
    }catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing JSON into ResourceInformation:\n$json\n",e)
    }
  }

}

private case class ResourceInformationJson(name: String, addresses: Seq[String]) {
  def toJValue: JValue = {
    Extraction.decompose(this)(DefaultFormats)
  }
  def toResourceInformation: ResourceInformation = {
    new ResourceInformation(name,addresses.toArray)
  }
}
