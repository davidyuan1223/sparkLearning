package org.apache.spark.internal.config
import java.util.{Map => JMap}
private [spark] trait ConfigProvider {
  def get(key: String): Option[String]
}

private [spark] class EnvProvider extends ConfigProvider {
  override def get(key: String): Option[String] = sys.env.get(key)
}

private [spark] class SystemProvider extends ConfigProvider {
  override def get(key: String): Option[String] = sys.props.get(key)
}

private [spark] class MapProvider(conf: JMap[String,String]) extends ConfigProvider {
  override def get(key: String): Option[String] = Option(conf.get(key))
}

private [spark] class SparkConfigProvider(conf: JMap[String,String]) extends ConfigProvider {
  override def get(key: String): Option[String] = {
    if (key.startsWith("spark.")) {
      Option(conf.get(key)).orElse(SparkConf.getDeprecatedConfig(key,conf))
    }else{
      None
    }
  }
}
