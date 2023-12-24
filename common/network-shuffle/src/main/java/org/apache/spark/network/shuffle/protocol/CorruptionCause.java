package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.shuffle.checksum.Cause;

public class CorruptionCause extends BlockTransferMessage{
    public Cause cause;

    public CorruptionCause(Cause cause) {
        this.cause = cause;
    }

    @Override
    protected Type type() {
        return Type.CORRUPTION_CAUSE;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("cause", cause)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CorruptionCause that = (CorruptionCause) o;
        return cause == that.cause;
    }

    @Override
    public int hashCode() {
        return cause.hashCode();
    }

    @Override
    public int encodedLength() {
        return 1; /* encoded length of cause */
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeByte(cause.ordinal());
    }

    public static CorruptionCause decode(ByteBuf buf) {
        int ordinal = buf.readByte();
        return new CorruptionCause(Cause.values()[ordinal]);
    }
}
