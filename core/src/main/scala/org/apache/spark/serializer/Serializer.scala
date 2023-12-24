package org.apache.spark.serializer

import org.apache.spark.annotation.{DeveloperApi, Private}
import org.apache.spark.util.NextIterator

import java.io.{Closeable, EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer
import javax.annotation.concurrent.NotThreadSafe
import scala.reflect.ClassTag

@DeveloperApi
abstract class Serializer {
  @volatile protected var defaultClassLoader: Option[ClassLoader] = None
  def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    defaultClassLoader=Some(classLoader)
    this
  }
  def newInstance(): SerializerInstance
  @Private
  private [spark] def supportsRelocationOfSerializedObjects: Boolean = false
}

@DeveloperApi
@NotThreadSafe
abstract class SerializerInstance {
  def serialize[T: ClassTag](t: T): ByteBuffer
  def deserialize[T: ClassTag](bytes: ByteBuffer): T
  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T
  def serializeStream(s: OutputStream): SerializationStream
  def deserializeStream(s: InputStream): DeserializationStream
}

@DeveloperApi
abstract class SerializationStream extends Closeable {
  def writeObject[T: ClassTag](t: T): SerializationStream
  def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)
  def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)
  def flush(): Unit
  override def close(): Unit
  def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    while (iter.hasNext){
      writeObject(iter.next())
    }
    this
  }
}


@DeveloperApi
abstract class DeserializationStream extends Closeable {
  def readObject[T: ClassTag](): T
  def readKey[T: ClassTag](): T = readObject[T]()
  def readValue[T: ClassTag](): T = readObject[T]()
  override def close(): Unit
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    override def getNext(): Any = {
      try {
        readObject[Any]()
      }catch {
        case eof: EOFException =>
          finished=true
          null
      }
    }

    override protected def close(): Unit = {
      DeserializationStream.this.close()
    }
  }
  def asKeyValueIterator: Iterator[(Any,Any)] = new NextIterator[(Any, Any)] {
    override protected def getNext(): (Any, Any) = {
      try {
        (readKey[Any](),readValue[Any]())
      }catch {
        case eof: EOFException =>
          finished=true
          null
      }
    }

    override protected def close(): Unit = {
      DeserializationStream.this.close()
    }
  }
}
