package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

public final class StreamChunkId implements Encodable{
    public final long streamId;
    public final int chunkIndex;

    public StreamChunkId(long streamId, int chunkIndex){
        this.streamId=streamId;
        this.chunkIndex=chunkIndex;
    }

    @Override
    public int encodedLength() {
        return 8+4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(streamId);
        buf.writeInt(chunkIndex);
    }

    public static StreamChunkId decode(ByteBuf buf){
        assert buf.readableBytes()>=8+4;
        long streamId = buf.readLong();
        int chunkIndex = buf.readInt();
        return new StreamChunkId(streamId,chunkIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, chunkIndex);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof StreamChunkId) {
            StreamChunkId o = (StreamChunkId) other;
            return streamId == o.streamId && chunkIndex == o.chunkIndex;
        }
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("streamId", streamId)
                .append("chunkIndex", chunkIndex)
                .toString();
    }
}
