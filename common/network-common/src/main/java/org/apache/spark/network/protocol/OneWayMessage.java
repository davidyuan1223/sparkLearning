package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

import java.util.Objects;

public final class OneWayMessage extends AbstractMessage implements RequestMessage{
    public OneWayMessage(ManagedBuffer body){
        super(body,true);
    }

    @Override
    public Type type() {
        return Type.OneWayMessage;
    }

    @Override
    public int encodedLength() {
        return 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeInt((int) body().size());
    }

    public static OneWayMessage decode(ByteBuf buf){
        buf.readInt();
        return new OneWayMessage(new NettyManagedBuffer(buf.duplicate()));
    }
    @Override
    public int hashCode() {
        return Objects.hashCode(body());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof OneWayMessage) {
            OneWayMessage o = (OneWayMessage) other;
            return super.equals(o);
        }
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("body", body())
                .toString();
    }
}
