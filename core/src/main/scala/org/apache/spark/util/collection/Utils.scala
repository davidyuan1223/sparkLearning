package org.apache.spark.util.collection

import com.google.common.collect.{Iterators => GuavaIterators, Ordering => GuavaOrdering}

import java.util
import java.util.Collections
import scala.collection.JavaConverters.{asJavaIterableConverter, asJavaIteratorConverter, asScalaIteratorConverter}
private [spark] object Utils {
  def takeOrdered[T](input: Iterator[T], num: Int)(implicit ord: Ordering[T]): Iterator[T] = {
    val ordering = new GuavaOrdering[T]{
      override def compare(t: T, t1: T): Int = ord.compare(t,t1)
    }
    ordering.leastOf(input.asJava, num).iterator.asScala
  }

  def mergeOrdered[T](inputs: Iterable[TraversableOnce[T]])(implicit ord: Ordering[T]): Iterator[T] = {
    val ordering = new GuavaOrdering[T] {
      override def compare(t: T, t1: T): Int = ord.compare(t,t1)
    }
    GuavaIterators.mergeSorted(inputs.map(_.toIterator.asJava).asJava, ordering).asScala
  }

  def sequenceToOption[T](input: Seq[Option[T]]): Option[Seq[T]] =
    if (input.forall(_.isDefined)) Some(input.flatten) else None

  def toMap[K,V](keys: Iterable[K],values: Iterable[V]): Map[K,V] = {
    val builder = Map.newBuilder[K,V]
    val keyIter = keys.iterator
    val valueIter = values.iterator
    while (keyIter.hasNext && valueIter.hasNext) {
      builder += (keyIter.next(),valueIter.next()).asInstanceOf[(K,V)]
    }
    builder.result()
  }

  def toMapWithIndex[K](keys: Iterable[K]): Map[K,Int] = {
    val builder = Map.newBuilder[K,Int]
    val keyIter = keys.iterator
    var idx = 0
    while (keyIter.hasNext){
      builder+=(keyIter.next(),idx).asInstanceOf[(K,Int)]
      idx=idx+1
    }
    builder.result()
  }

  def toJavaMap[K,V](keys: Iterable[K], values: Iterable[V]): java.util.Map[K,V] = {
    val map = new util.HashMap[K,V]()
    val keyIter = keys.iterator
    val valueIter = values.iterator
    while (keyIter.hasNext && valueIter.hasNext) {
      map.put(keyIter.next(), valueIter.next())
    }
    Collections.unmodifiableMap(map)
  }
}
