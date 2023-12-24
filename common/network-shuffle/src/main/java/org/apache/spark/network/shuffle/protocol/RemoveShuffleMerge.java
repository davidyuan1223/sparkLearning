package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

public class RemoveShuffleMerge extends BlockTransferMessage {
    public final String appId;
    public final int appAttemptId;
    public final int shuffleId;
    public final int shuffleMergeId;

    public RemoveShuffleMerge(
            String appId,
            int appAttemptId,
            int shuffleId,
            int shuffleMergeId) {
        this.appId = appId;
        this.appAttemptId = appAttemptId;
        this.shuffleId = shuffleId;
        this.shuffleMergeId = shuffleMergeId;
    }

    @Override
    protected Type type() {
        return Type.REMOVE_SHUFFLE_MERGE;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, appAttemptId, shuffleId, shuffleMergeId);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("appId", appId)
                .append("attemptId", appAttemptId)
                .append("shuffleId", shuffleId)
                .append("shuffleMergeId", shuffleMergeId)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof RemoveShuffleMerge) {
            RemoveShuffleMerge o = (RemoveShuffleMerge) other;
            return Objects.equal(appId, o.appId)
                    && appAttemptId == o.appAttemptId
                    && shuffleId == o.shuffleId
                    && shuffleMergeId == o.shuffleMergeId;
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId) + 4 + 4 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        buf.writeInt(appAttemptId);
        buf.writeInt(shuffleId);
        buf.writeInt(shuffleMergeId);
    }

    public static RemoveShuffleMerge decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        int attemptId = buf.readInt();
        int shuffleId = buf.readInt();
        int shuffleMergeId = buf.readInt();
        return new RemoveShuffleMerge(appId, attemptId, shuffleId, shuffleMergeId);
    }
}
