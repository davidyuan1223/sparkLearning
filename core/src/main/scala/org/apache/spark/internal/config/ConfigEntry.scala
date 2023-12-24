package org.apache.spark.internal.config

import org.apache.spark.internal.config.ConfigEntry.registerEntry

import java.util.concurrent.ConcurrentHashMap

private [spark] abstract class ConfigEntry [T](val key: String, val prependedKey: Option[String],
                                               val prependSeparator: String,
                                               val alternatives: List[String],
                                               val valueConverter: String => T,
                                               val stringConverter: T => String,
                                               val doc: String,
                                               val isPublic: Boolean,
                                               val version: String
                                              ) {
  registerEntry(this)
  def defaultValueString: String

  protected def readString(reader: ConfigReader): Option[String] = {
    val values = Seq(
      prependedKey.flatMap(reader.get(_)),
      alternatives.foldLeft(reader.get(key))((res, nextKey) => res.orElse(reader.get(nextKey)))
    ).flatten
    if (values.nonEmpty){
      Some(values.mkString(prependSeparator))
    }else{
      None
    }
  }
  def readFrom(reader: ConfigReader): T
  def defaultValue: Option[T] = None

  override def toString: String = {
    s"ConfigEntry(key=$key, defaultValue=$defaultValueString, doc=$doc, " +
      s"public=$isPublic, version=$version)"
  }
}

private class ConfigEntryWithDefault[T](key: String,
                                        prependedKey: Option[String],
                                        prependSeparator: String,
                                        alternative: List[String],
                                       _defaultValue: T,
                                        valueConverter: String => T,
                                        stringConverter: T => String,
                                        doc: String,
                                        isPublic: Boolean,
                                        version: String)extends ConfigEntry(
  key,
  prependedKey,
  prependSeparator,
  alternative,
  valueConverter,
  stringConverter,
  doc,
  isPublic,
  version
){
  override def defaultValue: Option[T] = Some(_defaultValue)

  override def defaultValueString: String = stringConverter(_defaultValue)
  def readFrom(reader: ConfigReader): T = {
    readString(reader).map(valueConverter).getOrElse(_defaultValue)
  }
}

private class ConfigEntryWithDefaultFunction[T](key: String,
                                                prependedKey: Option[String],
                                                prependSeparator: String,
                                                alternatives: List[String],
                                               _defaultFunction: () => T,
                                                valueConverter: String => T,
                                                stringConverter: T => String,
                                                doc: String,
                                                isPublic: Boolean,
                                                version: String)extends ConfigEntry(
  key,
  prependedKey,
  prependSeparator,
  alternatives,
  valueConverter,
  stringConverter,
  doc,
  isPublic,
  version
){
  override def defaultValue: Option[T] = Some(_defaultFunction())

  override def defaultValueString: String = stringConverter(_defaultFunction())

  override def readFrom(reader: ConfigReader): T = {
    readString(reader).map(valueConverter).getOrElse(_defaultFunction())
  }
}

private class ConfigEntryWithDefaultString[T](key: String,
                                                prependedKey: Option[String],
                                                prependSeparator: String,
                                                alternatives: List[String],
                                                _defaultValue: String,
                                                valueConverter: String => T,
                                                stringConverter: T => String,
                                                doc: String,
                                                isPublic: Boolean,
                                                version: String)extends ConfigEntry(
  key,
  prependedKey,
  prependSeparator,
  alternatives,
  valueConverter,
  stringConverter,
  doc,
  isPublic,
  version
){
  override def defaultValue: Option[T] = Some(valueConverter(_defaultValue))

  override def defaultValueString: String = _defaultValue

  override def readFrom(reader: ConfigReader): T = {
    val value = readString(reader).getOrElse(reader.substitute(_defaultValue))
    valueConverter(value)
  }
}

private [spark] class OptionalConfigEntry[T](key: String,
                                             prependedKey: Option[String],
                                             prependSeparator: String,
                                             alternatives: List[String],
                                             val rawValueConverter: String => T,
                                             val rawStringConverter: T => String,
                                             doc: String,
                                             isPublic: Boolean,
                                             version: String)extends ConfigEntry[Option[T]](
  key,
  prependedKey,
  prependSeparator,
  alternatives,
  s => Some(rawValueConverter(s)),
  v => v.map(rawStringConverter).orNull,
  doc,isPublic, version
){
  override def defaultValueString: String = ConfigEntry.UNDEFINED

  override def readFrom(reader: ConfigReader): Option[T] = {
    readString(reader).map(rawValueConverter)
  }
}

private [spark] class FallbackConfigEntry[T](key: String,
                                             prependedKey: Option[String],
                                             prependSeparator: String,
                                             alternatives: List[String],
                                             doc: String,
                                             isPublic: Boolean,
                                             version: String,
                                             val fallback: ConfigEntry[T]) extends ConfigEntry[T](
  key,prependedKey,prependSeparator,alternatives,fallback.valueConverter,fallback.stringConverter,doc, isPublic, version
){
  override def defaultValueString: String = s"<value of ${fallback.key}>"

  override def readFrom(reader: ConfigReader): T = {
    readString(reader).map(valueConverter).getOrElse(fallback.readFrom(reader))
  }
}


private [spark] object ConfigEntry {
  val UNDEFINED = "<undefined>"

  private [spark] val knownConfigs =
    new ConcurrentHashMap[String,ConfigEntry[_]]()

  def registerEntry(entry: ConfigEntry[_]): Unit = {
    val existing = knownConfigs.putIfAbsent(entry.key, entry)
    require(existing == null, s"Config entry ${entry.key} already registered!")
  }

  def findEntry(key: String): ConfigEntry[_] = knownConfigs.get(key)
}
