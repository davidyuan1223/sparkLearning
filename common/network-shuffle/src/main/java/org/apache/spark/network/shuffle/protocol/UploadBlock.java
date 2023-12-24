package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;
import java.util.Objects;

public class UploadBlock extends BlockTransferMessage {
    public final String appId;
    public final String execId;
    public final String blockId;
    public final byte[] metadata;
    public final byte[] blockData;

    public UploadBlock(String appId, String execId, String blockId, byte[] metadata, byte[] blockData) {
        this.appId = appId;
        this.execId = execId;
        this.blockId = blockId;
        this.metadata = metadata;
        this.blockData = blockData;
    }

    @Override
    protected Type type() {
        return Type.UPLOAD_BLOCK;
    }
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("appId", appId)
                .append("execId", execId)
                .append("blockId", blockId)
                .append("metadata size", metadata.length)
                .append("block size", blockData.length)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof UploadBlock) {
            UploadBlock o = (UploadBlock) other;
            return Objects.equals(appId, o.appId)
                    && Objects.equals(execId, o.execId)
                    && Objects.equals(blockId, o.blockId)
                    && Arrays.equals(metadata, o.metadata)
                    && Arrays.equals(blockData, o.blockData);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                + Encoders.Strings.encodedLength(execId)
                + Encoders.Strings.encodedLength(blockId)
                + Encoders.ByteArrays.encodedLength(metadata)
                + Encoders.ByteArrays.encodedLength(blockData);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        Encoders.Strings.encode(buf, blockId);
        Encoders.ByteArrays.encode(buf, metadata);
        Encoders.ByteArrays.encode(buf, blockData);
    }

    public static UploadBlock decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        String blockId = Encoders.Strings.decode(buf);
        byte[] metadata = Encoders.ByteArrays.decode(buf);
        byte[] blockData = Encoders.ByteArrays.decode(buf);
        return new UploadBlock(appId, execId, blockId, metadata, blockData);
    }
}
