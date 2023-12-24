package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;


public class OpenBlocks extends BlockTransferMessage {
    public final String appId;
    public final String execId;
    public final String[] blockIds;

    public OpenBlocks(String appId, String execId, String[] blockIds) {
        this.appId = appId;
        this.execId = execId;
        this.blockIds = blockIds;
    }

    @Override
    protected Type type() {
        return Type.OPEN_BLOCKS;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof OpenBlocks) {
            OpenBlocks o = (OpenBlocks) other;
            return java.util.Objects.equals(appId, o.appId)
                    && java.util.Objects.equals(execId, o.execId)
                    && Arrays.equals(blockIds, o.blockIds);
        }
        return false;
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
    public int hashCode() {
        return Objects.hashCode(appId,execId)*41+Arrays.hashCode(blockIds);
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                +Encoders.Strings.encodedLength(execId)
                +Encoders.StringArrays.encodedLength(blockIds);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf,appId);
        Encoders.Strings.encode(buf,execId);
        Encoders.StringArrays.encode(buf,blockIds);
    }

    public static OpenBlocks decode(ByteBuf buf){
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        String[] blockIds = Encoders.StringArrays.decode(buf);
        return new OpenBlocks(appId,execId,blockIds);
    }
}
