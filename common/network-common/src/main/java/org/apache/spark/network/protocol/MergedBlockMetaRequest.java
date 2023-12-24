package org.apache.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class MergedBlockMetaRequest extends AbstractMessage implements RequestMessage{
    public final long requestId;
    public final String appId;
    public final int shuffleId;
    public final int shuffleMergeId;
    public final int reduceId;

    public MergedBlockMetaRequest(long requestId, String appId, int shuffleId, int shuffleMergeId, int reduceId) {
        super(null,false);
        this.requestId = requestId;
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.shuffleMergeId = shuffleMergeId;
        this.reduceId = reduceId;
    }

    @Override
    public Type type() {
        return Type.MergedBlockMetaRequest;
    }

    @Override
    public int encodedLength() {
        return 8+Encoders.Strings.encodedLength(appId)+4+4+4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(requestId);
        Encoders.Strings.encode(buf,appId);
        buf.writeInt(shuffleId);
        buf.writeInt(shuffleMergeId);
        buf.writeInt(reduceId);
    }
    public static MergedBlockMetaRequest decode(ByteBuf buf){
        long requestId = buf.readLong();
        String appId = Encoders.Strings.decode(buf);
        int shuffleId = buf.readInt();
        int shuffleMergedId = buf.readInt();
        int reduceId = buf.readInt();
        return new MergedBlockMetaRequest(requestId,appId,shuffleId,shuffleMergedId,reduceId);
    }
    @Override
    public int hashCode() {
        return Objects.hashCode(requestId, appId, shuffleId, shuffleMergeId, reduceId);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof MergedBlockMetaRequest) {
            MergedBlockMetaRequest o = (MergedBlockMetaRequest) other;
            return requestId == o.requestId && shuffleId == o.shuffleId &&
                    shuffleMergeId == o.shuffleMergeId && reduceId == o.reduceId &&
                    Objects.equal(appId, o.appId);
        }
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("requestId", requestId)
                .append("appId", appId)
                .append("shuffleId", shuffleId)
                .append("shuffleMergeId", shuffleMergeId)
                .append("reduceId", reduceId)
                .toString();
    }
}
