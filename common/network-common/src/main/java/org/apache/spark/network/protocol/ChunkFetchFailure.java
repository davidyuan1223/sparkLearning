package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

public final class ChunkFetchFailure extends AbstractMessage implements ResponseMessage{
    public final StreamChunkId streamChunkId;
    public final String errorString;
    public ChunkFetchFailure(StreamChunkId streamChunkId, String errorString){
        this.streamChunkId=streamChunkId;
        this.errorString=errorString;
    }

    @Override
    public Type type() {
        return Type.ChunkFetchFailure;
    }

    @Override
    public int encodedLength() {
        return streamChunkId.encodedLength()+Encoders.Strings.encodedLength(errorString);
    }

    @Override
    public void encode(ByteBuf buf) {
        streamChunkId.encode(buf);
        Encoders.Strings.encode(buf,errorString);
    }
    public static ChunkFetchFailure decode(ByteBuf buf){
        StreamChunkId streamChunkId = StreamChunkId.decode(buf);
        String errorString = Encoders.Strings.decode(buf);
        return new ChunkFetchFailure(streamChunkId,errorString);
    }
    @Override
    public int hashCode() {
        return Objects.hash(streamChunkId, errorString);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ChunkFetchFailure) {
            ChunkFetchFailure o = (ChunkFetchFailure) other;
            return streamChunkId.equals(o.streamChunkId) && errorString.equals(o.errorString);
        }
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("streamChunkId", streamChunkId)
                .append("errorString", errorString)
                .toString();
    }
}
