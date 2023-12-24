package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

public class BlocksRemoved extends BlockTransferMessage {
    public final int numRemovedBlocks;

    public BlocksRemoved(int numRemovedBlocks) {
        this.numRemovedBlocks = numRemovedBlocks;
    }

    @Override
    protected Type type() { return Type.BLOCKS_REMOVED; }

    @Override
    public int hashCode() {
        return Objects.hashCode(numRemovedBlocks);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("numRemovedBlocks", numRemovedBlocks)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof BlocksRemoved) {
            BlocksRemoved o = (BlocksRemoved) other;
            return numRemovedBlocks == o.numRemovedBlocks;
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeInt(numRemovedBlocks);
    }

    public static BlocksRemoved decode(ByteBuf buf) {
        int numRemovedBlocks = buf.readInt();
        return new BlocksRemoved(numRemovedBlocks);
    }
}
