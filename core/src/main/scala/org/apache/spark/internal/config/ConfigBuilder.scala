package org.apache.spark.internal.config

import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.util.Utils

import java.util.concurrent.TimeUnit
import java.util.regex.PatternSyntaxException
import scala.concurrent.duration.TimeUnit
import scala.util.matching.Regex

private object ConfigHelpers {
  def toNumber[T](s: String, converter: String => T,key: String, configType: String): T = {
    try {
      converter(s.trim)
    }catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$key should be $configType, but was $s")
    }
  }

  def toBoolean(s: String, key: String): Boolean = {
    try {
      s.trim.toBoolean
    }catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s")
    }
  }

  def stringToSeq[T](str: String, converter: String => T): Seq[T] = {
    Utils.stringToSeq(str).map(converter)
  }

  def seqToString[T](v: Seq[T], stringConverter: T => String): String = {
    v.map(stringConverter).mkString(",")
  }

  def timeFromString(str: String, unit: TimeUnit): Long = JavaUtils.timeStringAs(str,unit)

  def timeToString(v: Long, unit: TimeUnit): String = TimeUnit.MILLISECONDS.convert(v, unit)+"ms"

  def byteFromString(str: String, unit: ByteUnit): Long = {
    val (input,multiplier) =
      if(str.nonEmpty && str.charAt(0) == '-') {
        (str.substring(1),-1)
      }else{
        (str,1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
  }

  def byteToString(v: Long, unit: ByteUnit): String = unit.convertTo(v, ByteUnit.BYTE) + "b"

  def regexFromString(str: String, key: String): Regex = {
    try str.r catch {
      case e: PatternSyntaxException =>
        throw new IllegalArgumentException(s"$key should be a regex, but was $str",e)
    }
  }
}


private [spark] class TypedConfigBuilder[T](val parent: ConfigBuilder,val converter: String => T,
                                            val stringConverter: T => String) {
  import ConfigHelpers._

  def this(parent: ConfigBuilder, converter: String => T) = {
    this(parent,converter,Option(_).map(_.toString).orNull)
  }

  def transform(fn: T => T): TypedConfigBuilder[T] = {
    new TypedConfigBuilder(parent, s => fn(converter(s)), stringConverter)
  }

  def checkValue(validator: T => Boolean, errorMsg: String): TypedConfigBuilder[T] = {
    transform{ v =>
      if (!validator(v)) {
        throw new IllegalArgumentException(s"'$v' in ${parent.key} is invalid. $errorMsg'")
      }
      v
    }
  }

  def checkValues(validValues: Set[T]): TypedConfigBuilder[T] = {
    transform{ v =>
      if(!validValues.contains(v)) {
        throw new IllegalArgumentException(
          s"The value of ${parent.key} should be one of ${validValues.mkString(", ")}, but was $v"
        )
      }
      v
    }
  }


  def toSequence: TypedConfigBuilder[Seq[T]] = {
    new TypedConfigBuilder(parent, stringToSeq(_,converter),seqToString(_,stringConverter))
  }

  def createOptional: OptionalConfigEntry[T] = {
    val entry = new OptionalConfigEntry[T](parent.key, parent._prependedKey,
      parent._prependSeparator,parent._alternatives,converter,stringConverter,parent._doc,
      parent._public,parent._version)
    parent._onCreate.foreach(_(entry))
    entry
  }

  def createWithDefault(default: T): ConfigEntry[T] = {
    default match {
      case str: String => createWithDefaultString(str)
      case _ =>
        val transformedDefault = converter(stringConverter(default))
        val entry = new ConfigEntryWithDefault[T](parent.key, parent._prependedKey,
          parent._prependSeparator, parent._alternatives, transformedDefault, converter,
          stringConverter, parent._doc, parent._public, parent._version)
        parent._onCreate.foreach(_ (entry))
        entry
    }
  }

  def createWithDefaultString(default: String): ConfigEntry[T] = {
    val entry = new ConfigEntryWithDefaultString[T](parent.key, parent._prependedKey,parent._prependSeparator,
      parent._alternatives,default,converter,stringConverter,parent._doc,parent._public,parent._version)
    parent._onCreate.foreach(_(entry))
    entry
  }

  def createWithDefaultFunction(defaultFunc: () => T): ConfigEntry[T] = {
    val entry = new ConfigEntryWithDefaultFunction[T](parent.key, parent._prependedKey,
      parent._prependSeparator,parent._alternatives,defaultFunc,converter,stringConverter,
      parent._doc,parent._public,parent._version)
    parent._onCreate.foreach(_(entry))
    entry
  }
}
private [spark] case class ConfigBuilder(key: String) {
  import ConfigHelpers._
  private [config] var _prependedKey: Option[String] = None
  private [config] var _prependSeparator: String = ""
  private [config] var _public = true
  private [config] var _doc = ""
  private [config] var _version = ""
  private [config] var _onCreate: Option[ConfigEntry[_] => Unit] = None
  private [config] var _alternatives = List.empty[String]

  def internal(): ConfigBuilder = {
    _public = false
    this
  }

  def doc(s: String): ConfigBuilder = {
    _doc = s
    this
  }

  def version(v: String): ConfigBuilder = {
    _version = v
    this
  }

  def onCreate(callback: ConfigEntry[_] => Unit): ConfigBuilder = {
    _onCreate = Option(callback)
    this
  }

  def withPrepended(key: String, separator: String = " "): ConfigBuilder = {
    _prependedKey = Option(key)
    _prependSeparator = separator
    this
  }

  def withAlternative(key: String): ConfigBuilder = {
    _alternatives = _alternatives :+ key
    this
  }

  private def checkPrependConfig: Unit = {
    if (_prependedKey.isDefined) {
      throw new IllegalArgumentException(s"$key type must be string if prepend used")
    }
  }

  def intConf: TypedConfigBuilder[Int] = {
    checkPrependConfig
    new TypedConfigBuilder(this, toNumber(_,_.toInt,key,"int"))
  }

  def longConf: TypedConfigBuilder[Long] = {
    checkPrependConfig
    new TypedConfigBuilder(this, toNumber(_, _.toLong, key, "long"))
  }

  def doubleConf: TypedConfigBuilder[Double] = {
    checkPrependConfig
    new TypedConfigBuilder(this, toNumber(_, _.toDouble, key, "double"))
  }

  def booleanConf: TypedConfigBuilder[Boolean] = {
    checkPrependConfig
    new TypedConfigBuilder(this, toBoolean(_, key))
  }

  def stringConf: TypedConfigBuilder[String] = {
    new TypedConfigBuilder(this, v => v)
  }

  def timeConf(unit: TimeUnit): TypedConfigBuilder[Long] = {
    checkPrependConfig
    new TypedConfigBuilder(this, timeFromString(_, unit), timeToString(_, unit))
  }

  def bytesConf(unit: ByteUnit): TypedConfigBuilder[Long] = {
    checkPrependConfig
    new TypedConfigBuilder(this, byteFromString(_, unit), byteToString(_, unit))
  }

  def fallbackConf[T](fallback: ConfigEntry[T]): ConfigEntry[T] = {
    val entry = new FallbackConfigEntry(key, _prependedKey, _prependSeparator, _alternatives, _doc,
      _public, _version, fallback)
    _onCreate.foreach(_(entry))
    entry
  }

  def regexConf: TypedConfigBuilder[Regex] = {
    checkPrependConfig
    new TypedConfigBuilder(this, regexFromString(_, this.key), _.toString)
  }
}
