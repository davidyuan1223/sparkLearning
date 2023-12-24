package org.apache.spark.util.io

import org.apache.spark.network.util.AbstractFileRegion

import java.nio.channels.WritableByteChannel

private [io] class ChunkedByteBufferFileRegion(private val chunkedByteBuffer: ChunkedByteBuffer,
                                       private val ioChunkSize: Int)extends AbstractFileRegion{
  private var _transferred: Long = 0
  private val chunks = chunkedByteBuffer.getChunks()
  private val size = chunks.foldLeft(0L) { _ + _.remaining()}
  protected def deallocate: Unit = {}
  override def count(): Long = size
  override def position(): Long = 0
  override def transferred(): Long = _transferred

  private var currentChunkIdx=0
  def transferTo(target: WritableByteChannel, position: Long): Long ={
    assert(position == _transferred)
    if (position == size) return 0L
    var keepGoing = true
    var written = 0L
    var currentChunk = chunks(currentChunkIdx)
    while (keepGoing) {
      while (currentChunk.hasRemaining && keepGoing) {
        val ioSize = math.min(currentChunk.remaining(),ioChunkSize)
        val originalLimit = currentChunk.limit()
        currentChunk.limit(currentChunk.position()+ioSize)
        val thisWriteSize = target.write(currentChunk)
        currentChunk.limit(originalLimit)
        written+=thisWriteSize
        if (thisWriteSize<ioSize){
          keepGoing=false
        }
      }
      if (keepGoing){
        currentChunkIdx+=1
        if (currentChunkIdx==chunks.length){
          keepGoing=false
        }else{
          currentChunk=chunks(currentChunkIdx)
        }
      }
    }
    _transferred+=written
    written
  }
}
