package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

import java.util.Objects;

public final class ChunkFetchSuccess extends AbstractResponseMessage{
    public final StreamChunkId streamChunkId;
    public ChunkFetchSuccess(StreamChunkId streamChunkId, ManagedBuffer buffer){
        super(buffer,true);
        this.streamChunkId=streamChunkId;
    }

    @Override
    public Type type() {
        return Type.ChunkFetchSuccess;
    }

    @Override
    public int encodedLength() {
        return streamChunkId.encodedLength();
    }

    @Override
    public void encode(ByteBuf buf) {
        streamChunkId.encode(buf);
    }

    @Override
    public ResponseMessage createFailureResponse(String error) {
        return new ChunkFetchFailure(streamChunkId,error);
    }
    public static ChunkFetchSuccess decode(ByteBuf buf){
        StreamChunkId streamChunkId = StreamChunkId.decode(buf);
        buf.retain();
        NettyManagedBuffer managedBuffer = new NettyManagedBuffer(buf.duplicate());
        return new ChunkFetchSuccess(streamChunkId,managedBuffer);
    }
    @Override
    public int hashCode() {
        return Objects.hash(streamChunkId, body());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ChunkFetchSuccess) {
            ChunkFetchSuccess o = (ChunkFetchSuccess) other;
            return streamChunkId.equals(o.streamChunkId) && super.equals(o);
        }
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("streamChunkId", streamChunkId)
                .append("buffer", body())
                .toString();
    }
}
