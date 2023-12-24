package org.apache.spark.util.io

import java.io.OutputStream
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer

private [spark] class ChunkedByteBufferOutputStream(chunkSize: Int,
                                                    allocator: Int => ByteBuffer) extends OutputStream{
  private [this] var toChunkedByteBufferWasCalled = false
  private val chunks = new ArrayBuffer[ByteBuffer]
  private [this] var lastChunkIndex = -1
  private [this] var position = chunkSize
  private [this] var _size = 0L
  private [this] var closed: Boolean = false
  def size: Long = _size

  override def close(): Unit = {
    if (!closed){
      super.close()
      closed=true
    }
  }

  override def write(b: Int): Unit = {
    require(!closed, "cannot write a closed ChunkedByteBufferOutputStream")
    allocateNewChunkIfNeeded()
    chunks(lastChunkIndex).put(b.toByte)
    position+=1
    _size+=1
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    require(!closed, "cannot write to a closed ChunkedByteBufferOutputStream")
    var written = 0
    while (written < len) {
      allocateNewChunkIfNeeded()
      val thisBatch = math.min(chunkSize - position, len-written)
      chunks(lastChunkIndex).put(b, written+off, thisBatch)
      written+=thisBatch
      position+=thisBatch
    }
    _size+=len
  }

  @inline
  private def allocateNewChunkIfNeeded(): Unit = {
    if (position == chunkSize) {
      chunks += allocator(chunkSize)
      lastChunkIndex += 1
      position = 0
    }
  }

  def toChunkedByteBuffer: ChunkedByteBuffer = {
    require(closed, "cannot call toChunkedByteBuffer() unless close() has been called")
    require(!toChunkedByteBufferWasCalled, "toChunkedByteBuffer() can only be called once")
    toChunkedByteBufferWasCalled=true
    if (lastChunkIndex == -1){
      new ChunkedByteBuffer(Array.empty[ByteBuffer])
    }else {
      val ret = new Array[ByteBuffer](chunks.size)
      for(i <- 0 until chunks.size-1 ){
        ret(i) = chunks(i)
        ret(i).flip()
      }else{
        ret(lastChunkIndex)=allocator(position)
        chunks(lastChunkIndex).flip()
        ret(lastChunkIndex).put(chunks(lastChunkIndex))
        ret(lastChunkIndex).flip()
        StorageUtil.dispose(chunks(lastChunkIndex))
      }
      new ChunkedByteBuffer(ret)
    }
  }
}
