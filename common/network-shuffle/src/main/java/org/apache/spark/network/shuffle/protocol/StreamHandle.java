package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

public class StreamHandle extends BlockTransferMessage{
    public final long streamId;
    public final int numChunks;

    public StreamHandle(long streamId, int numChunks) {
        this.streamId = streamId;
        this.numChunks = numChunks;
    }

    @Override
    protected Type type() { return Type.STREAM_HANDLE; }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, numChunks);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("streamId", streamId)
                .append("numChunks", numChunks)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof StreamHandle) {
            StreamHandle o = (StreamHandle) other;
            return Objects.equals(streamId, o.streamId)
                    && Objects.equals(numChunks, o.numChunks);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return 8 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(streamId);
        buf.writeInt(numChunks);
    }

    public static StreamHandle decode(ByteBuf buf) {
        long streamId = buf.readLong();
        int numChunks = buf.readInt();
        return new StreamHandle(streamId, numChunks);
    }
}
