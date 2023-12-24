package org.apache.spark.internal.config
import java.util.{Map => JMap}
import scala.collection.mutable
import scala.util.matching.Regex
private object ConfigReader {
  private val REF_RF = "\\$\\{(?:(\\w+?):)?(\\S+?)\\}".r
}

private [spark] class ConfigReader(conf: ConfigProvider) {
  def this(conf: JMap[String,String]) = this(new MapProvider(conf))
  private val bindings = new mutable.HashMap[String,ConfigProvider]()
  bind(null,conf)
  bindEnv(new EnvProvider)
  bindSystem(new SystemProvider)

  def bind(prefix: String, provider: ConfigProvider): ConfigReader = {
    bindings(prefix) = provider
    this
  }
  def bind(prefix: String, values: JMap[String,String]): ConfigReader = {
    bind(prefix, new MapProvider(values))
  }
  def bindEnv(provider: EnvProvider): ConfigReader = bind("env",provider)
  def bindSystem(provider: SystemProvider): ConfigReader = bind("system",provider)
  def get(key: String): Option[String] = conf.get(key).map(substitute)
  def substitute(input: String): String = substitute(input, Set())
  private def substitute(input: String, usedRefs: mutable.Set[String]): String = {
    if (input!=null){
      ConfigReader.REF_RF.replaceAllIn(input,{ m =>
        val prefix = m.group(1)
        val name = m.group(2)
        val ref = if(prefix==null) name else s"$prefix:$name"
        require(!usedRefs.contains(ref), s"Circular reference in $input: $ref")
        val replacement = bindings.get(prefix)
          .flatMap(getOrDefault(_,name))
          .map{v => substitute(v, usedRefs+ref)}
          .getOrElse(m.matched)
        Regex.quoteReplacement(replacement)
      })
    }else{
      input
    }
  }

  private def getOrDefault(conf: ConfigProvider, key: String): Option[String] = {
    conf.get(key).orElse{
      ConfigEntry.findEntry(key) match {
        case e: ConfigEntryWithDefault[_] => Option(e.defaultValueString)
        case e: ConfigEntryWithDefaultString[_] => Option(e.defaultValueString)
        case e: ConfigEntryWithDefaultFunction[_] => Option(e.defaultValueString)
        case e: FallbackConfigEntry[_] => getOrDefault(conf, e.fallback.key)
        case _ => None
      }
    }
  }
}
