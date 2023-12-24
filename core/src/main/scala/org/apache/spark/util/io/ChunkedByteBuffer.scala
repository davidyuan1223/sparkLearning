package org.apache.spark.util.io

import com.google.common.io.ByteStreams
import com.google.common.primitives.UnsignedBytes
import org.apache.commons.io.IOUtils
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.util.{ByteArrayWritableChannel, LimitedInputStream}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.Utils

import java.io.{Externalizable, File, FileInputStream, InputStream, ObjectInput, ObjectOutput}
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

private [spark] class ChunkedByteBuffer(var chunks: Array[ByteBuffer]) extends Externalizable{
  require(chunks!=null, "chunks must not be null")
  require(!chunks.contains(null), "chunks must not contain null")
  require(chunks.forall(_.position()==0), "chunks' positions must be 0")
  private var bufferWriteChunkSize =
    Option(SparkEnv.get).map(_.conf.get(config.BUFFER_WRITE_CHUNK_SIZE))
      .getOrElse(config.BUFFER_WRITE_CHUNK_SIZE.defaultValue.get).toInt

  private [this] var disposed: Boolean = false
  private var _size: Long = chunks.map(_.limit().asInstanceOf[Long]).sum
  def size: Long = _size
  def this() = {
    this(Array.empty[ByteBuffer])
  }
  def this(byteBuffer: ByteBuffer) = {
    this(Array(byteBuffer))
  }
  def writeFully(channel: WritableByteChannel): Unit = {
    for(bytes <- getChunks()) {
      val originalLimit = bytes.limit()
      while(bytes.hasRemaining) {
        val ioSize = Math.min(bytes.remaining(), bufferWriteChunkSize)
        bytes.limlit(bytes.position() + ioSize)
        channel.write(bytes)
        bytes.limit(originalLimit)
      }
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(chunks.length)
    val chunksCopy = getChunks()
    chunksCopy.foreach(buffer => out.writeInt(buffer.limit()))
    chunksCopy.foreach(Utils.writeByteBuffer(_,out))
  }

  override def readExternal(in: ObjectInput): Unit = {
    val chunksNum = in.readInt()
    val indices = 0 until chunksNum
    val chunksSize = indices.map(_ => in.readInt())
    val chunks = new Array[ByteBuffer](chunksNum)
    indices.foreach{ i =>
      val chunkSize = chunksSize(i)
      chunks(i) = {
        val arr = new Array[Byte](chunkSize)
        in.readFully(arr, 0, chunkSize)
        ByteBuffer.wrap(arr)
      }
    }
    this.chunks=chunks
    this._size=chunks.map(_.limit().toLong).sum
  }

  def toNetty: ChunkedByteBufferFileRegion = {
    new ChunkedByteBufferFileRegion(this, bufferWriteChunkSize)
  }

  def toArray: Array[Byte] = {
    if (size >= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
      throw new UnsupportedOperationException(
        s"cannot call toArray because buffer size ($size bytes) exceeds maximum array size")
    }
    val byteChannel = new ByteArrayWritableChannel(size.toInt)
    writeFully(byteChannel)
    byteChannel.close()
    byteChannel.getData
  }

  def toByteBuffer: ByteBuffer = {
    if (chunks.length==1) {
      chunks.head.duplicate()
    }else {
      ByteBuffer.wrap(toArray)
    }
  }

  def toInputStream(dispose: Boolean = false): InputStream = {
    new ChunkedByteBufferInputStream(this,dispose)
  }

  def getChunks(): Array[ByteBuffer] = {
    chunks.map(_.duplicate())
  }

  def copy(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    val copiedChunks = getChunks().map{chunk =>
      val newChunk = allocator(chunk.limit())
      newChunk.put(chunk)
      newChunk.flip()
      newChunk
    }
    new ChunkedByteBuffer(copiedChunks)
  }

  def dispose(): Unit = {
    if (!disposed){
      chunks.foreach(StorageUtils.dispose)
      disposed=true
    }
  }
}

private [spark] object ChunkedByteBuffer {
  private val CHUNK_BUFFER_SIZE = 1024*1024
  private val MINIMUM_CHUNK_BUFFER_SIZE = 1024

  def fromManagedBuffer(data: ManagedBuffer): ChunkedByteBuffer = {
    data match {
      case f: FileSegmentManagedBuffer =>
        fromFile(f.getFile,f.getOffset,f.getLength)
      case e: EncryptedManagedBuffer =>
        e.blockData.toChunkedByteBuffer(ByteBuffer.allocate _)
      case other =>
        new ChunkedByteBuffer(other.nioByteBuffer())
    }
  }

  def fromFile(file: File): ChunkedByteBuffer = {
    fromFile(file,0,file.length())
  }

  private def fromFile(file: File, offset: Long, length: Long): ChunkedByteBuffer = {
    val is = new FileInputStream(file)
    ByteStreams.skipFully(is,offset)
    val in = new LimitedInputStream(is,length)
    val chunkSize = math.min(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,length).toInt
    val out = new ChunkedByteBufferOutputStream(chunkSize,ByteBuffer.allocate _)
    Utils.tryWithSafeFinally{
      IOUtils.copy(in,out)
    }{
      in.close()
      out.close()
    }
    out.toChunkedByteBuffer
  }

  def estimateBufferChunkSize(estimatedSize: Long = -1):Int = {
    if (estimatedSize<0){
      CHUNK_BUFFER_SIZE
    }else{
      math.max(math.min(estimatedSize,CHUNK_BUFFER_SIZE).toInt,MINIMUM_CHUNK_BUFFER_SIZE)
    }
  }
}

private [spark] class ChunkedByteBufferInputStream
(var chunkedByteBuffer: ChunkedByteBuffer, dispose: Boolean) extends InputStream {
  private [this] var chunks = chunkedByteBuffer.getChunks().filter(_.hasRemaining).iterator
  private [this] var currentChunk: ByteBuffer = {
    if (chunks.hasNext){
      chunks.next()
    }else{
      null
    }
  }

  override def read(): Int = {
    if (currentChunk!=null && !currentChunk.hasRemaining && chunks.hasNext){
      currentChunk=chunks.next()
    }
    if (currentChunk!=null && currentChunk.hasRemaining) {
      UnsignedBytes.toInt(currentChunk.get())
    }else{
      close()
      -1
    }
  }

  override def read(dest: Array[Byte], off: Int, len: Int): Int = {
    if (currentChunk!=null && !currentChunk.hasRemaining && chunks.hasNext){
      currentChunk=chunks.next()
    }
    if (currentChunk!=null && currentChunk.hasRemaining){
      val amountToGet = math.min(currentChunk.remaining(), len)
      currentChunk.get(dest,off,amountToGet)
      amountToGet
    }else {
      close()
      -1
    }
  }

  override def skip(bytes: Long): Long = {
    if (currentChunk!=null) {
      val amountToSkip = math.min(bytes,currentChunk.remaining()).toInt
      currentChunk.position(currentChunk.position()+amountToSkip)
      if (currentChunk.remaining()==0) {
        if (chunks.hasNext){
          currentChunk=chunks.next()
        }else{
          close()
        }
      }
      amountToSkip
    }else {
      0L
    }
  }

  override def close(): Unit = {
    if (chunkedByteBuffer !=null && dispose) {
      chunkedByteBuffer.dispose()
    }
    chunkedByteBuffer=null
    chunks=null
    currentChunk=null
  }
}
