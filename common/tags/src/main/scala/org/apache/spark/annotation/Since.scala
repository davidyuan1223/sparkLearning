package org.apache.spark.annotation

import scala.annotation.StaticAnnotation
import scala.annotation.meta.{beanGetter, field, getter, param, setter}

@param @field @getter @setter @beanGetter @beanGetter
private[spark] class Since(version: String) extends StaticAnnotation{

}
