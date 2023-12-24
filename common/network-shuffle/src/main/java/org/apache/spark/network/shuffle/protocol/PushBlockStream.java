package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

public class PushBlockStream extends BlockTransferMessage {
    public final String appId;
    public final int appAttemptId;
    public final int shuffleId;
    public final int shuffleMergeId;
    public final int mapIndex;
    public final int reduceId;
    // Similar to the chunkIndex in StreamChunkId, indicating the index of a block in a batch of
    // blocks to be pushed.
    public final int index;

    public PushBlockStream(
            String appId,
            int appAttemptId,
            int shuffleId,
            int shuffleMergeId,
            int mapIndex,
            int reduceId,
            int index) {
        this.appId = appId;
        this.appAttemptId = appAttemptId;
        this.shuffleId = shuffleId;
        this.shuffleMergeId = shuffleMergeId;
        this.mapIndex = mapIndex;
        this.reduceId = reduceId;
        this.index = index;
    }

    @Override
    protected Type type() {
        return Type.PUSH_BLOCK_STREAM;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, appAttemptId, shuffleId, shuffleMergeId, mapIndex , reduceId,
                index);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("appId", appId)
                .append("attemptId", appAttemptId)
                .append("shuffleId", shuffleId)
                .append("shuffleMergeId", shuffleMergeId)
                .append("mapIndex", mapIndex)
                .append("reduceId", reduceId)
                .append("index", index)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PushBlockStream) {
            PushBlockStream o = (PushBlockStream) other;
            return Objects.equal(appId, o.appId)
                    && appAttemptId == o.appAttemptId
                    && shuffleId == o.shuffleId
                    && shuffleMergeId == o.shuffleMergeId
                    && mapIndex == o.mapIndex
                    && reduceId == o.reduceId
                    && index == o.index;
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId) + 4 + 4 + 4 + 4 + 4 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        buf.writeInt(appAttemptId);
        buf.writeInt(shuffleId);
        buf.writeInt(shuffleMergeId);
        buf.writeInt(mapIndex);
        buf.writeInt(reduceId);
        buf.writeInt(index);
    }

    public static PushBlockStream decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        int attemptId = buf.readInt();
        int shuffleId = buf.readInt();
        int shuffleMergeId = buf.readInt();
        int mapIdx = buf.readInt();
        int reduceId = buf.readInt();
        int index = buf.readInt();
        return new PushBlockStream(appId, attemptId, shuffleId, shuffleMergeId, mapIdx, reduceId,
                index);
    }
}
