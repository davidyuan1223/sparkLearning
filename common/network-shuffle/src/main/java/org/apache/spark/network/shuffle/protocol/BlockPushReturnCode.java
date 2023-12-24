package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.server.BlockPushNonFatalFailure;

import java.util.Objects;

public class BlockPushReturnCode extends BlockTransferMessage {
    public final byte returnCode;
    // Block ID of the block that experiences a non-fatal block push failure.
    // Will be an empty string for any successfully pushed block.
    public final String failureBlockId;

    public BlockPushReturnCode(byte returnCode, String failureBlockId) {
        Preconditions.checkNotNull(BlockPushNonFatalFailure.getReturnCode(returnCode));
        this.returnCode = returnCode;
        this.failureBlockId = failureBlockId;
    }

    @Override
    protected Type type() {
        return Type.PUSH_BLOCK_RETURN_CODE;
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnCode, failureBlockId);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("returnCode", returnCode)
                .append("failureBlockId", failureBlockId)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof BlockPushReturnCode) {
            BlockPushReturnCode o = (BlockPushReturnCode) other;
            return returnCode == o.returnCode && Objects.equals(failureBlockId, o.failureBlockId);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return 1 + Encoders.Strings.encodedLength(failureBlockId);
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeByte(returnCode);
        Encoders.Strings.encode(buf, failureBlockId);
    }

    public static BlockPushReturnCode decode(ByteBuf buf) {
        byte type = buf.readByte();
        String failureBlockId = Encoders.Strings.decode(buf);
        return new BlockPushReturnCode(type, failureBlockId);
    }
}
