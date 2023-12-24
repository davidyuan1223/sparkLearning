package org.apache.spark.util

object Iterators {
  def size(iterator: Iterator[_]): Long = {
    var count = 0L
    while (iterator.hasNext){
      count += 1L
      iterator.next()
    }
    count
  }
}
