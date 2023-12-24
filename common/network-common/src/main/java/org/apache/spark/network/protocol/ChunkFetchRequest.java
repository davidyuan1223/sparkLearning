package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public final class ChunkFetchRequest extends AbstractMessage implements RequestMessage{
    public final StreamChunkId streamChunkId;
    public ChunkFetchRequest(StreamChunkId streamChunkId){
        this.streamChunkId=streamChunkId;
    }

    @Override
    public Type type() {
        return Type.ChunkFetchRequest;
    }

    @Override
    public int encodedLength() {
        return streamChunkId.encodedLength();
    }

    @Override
    public void encode(ByteBuf buf) {
        streamChunkId.encode(buf);
    }

    public static ChunkFetchRequest decode(ByteBuf buf){
        return new ChunkFetchRequest(StreamChunkId.decode(buf));
    }

    @Override
    public int hashCode() {
        return streamChunkId.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ChunkFetchRequest) {
            ChunkFetchRequest o = (ChunkFetchRequest) other;
            return streamChunkId.equals(o.streamChunkId);
        }
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("streamChunkId", streamChunkId)
                .toString();
    }
}
