package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;
import java.util.Objects;

public class UploadBlockStream extends BlockTransferMessage {
    public final String blockId;
    public final byte[] metadata;

    public UploadBlockStream(String blockId, byte[] metadata) {
        this.blockId = blockId;
        this.metadata = metadata;
    }

    @Override
    protected Type type() { return Type.UPLOAD_BLOCK_STREAM; }

    @Override
    public int hashCode() {
        int objectsHashCode = Objects.hashCode(blockId);
        return objectsHashCode * 41 + Arrays.hashCode(metadata);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("blockId", blockId)
                .append("metadata size", metadata.length)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof UploadBlockStream) {
            UploadBlockStream o = (UploadBlockStream) other;
            return Objects.equals(blockId, o.blockId)
                    && Arrays.equals(metadata, o.metadata);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(blockId)
                + Encoders.ByteArrays.encodedLength(metadata);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, blockId);
        Encoders.ByteArrays.encode(buf, metadata);
    }

    public static UploadBlockStream decode(ByteBuf buf) {
        String blockId = Encoders.Strings.decode(buf);
        byte[] metadata = Encoders.ByteArrays.decode(buf);
        return new UploadBlockStream(blockId, metadata);
    }
}
