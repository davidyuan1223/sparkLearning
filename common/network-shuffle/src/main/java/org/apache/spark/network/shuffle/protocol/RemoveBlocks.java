package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;
import java.util.Objects;

public class RemoveBlocks extends BlockTransferMessage{
    public final String appId;
    public final String execId;
    public final String[] blockIds;

    public RemoveBlocks(String appId, String execId, String[] blockIds) {
        this.appId = appId;
        this.execId = execId;
        this.blockIds = blockIds;
    }

    @Override
    protected Type type() { return Type.REMOVE_BLOCKS; }

    @Override
    public int hashCode() {
        return Objects.hash(appId, execId) * 41 + Arrays.hashCode(blockIds);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("appId", appId)
                .append("execId", execId)
                .append("blockIds", Arrays.toString(blockIds))
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RemoveBlocks) {
            RemoveBlocks o = (RemoveBlocks) other;
            return Objects.equals(appId, o.appId)
                    && Objects.equals(execId, o.execId)
                    && Arrays.equals(blockIds, o.blockIds);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                + Encoders.Strings.encodedLength(execId)
                + Encoders.StringArrays.encodedLength(blockIds);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        Encoders.StringArrays.encode(buf, blockIds);
    }

    public static RemoveBlocks decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        String[] blockIds = Encoders.StringArrays.decode(buf);
        return new RemoveBlocks(appId, execId, blockIds);
    }
}
