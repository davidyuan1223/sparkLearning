package org.apache.spark.network.sasl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.spark.network.buffer.NettyManagedBuffer;
import org.apache.spark.network.protocol.AbstractMessage;
import org.apache.spark.network.protocol.Encoders;

public class SaslMessage extends AbstractMessage {
    private static final byte TAG_BYTE=(byte)0xEA;
    public final String appId;
    SaslMessage(String appId,byte[] message){
        this(appId, Unpooled.wrappedBuffer(message));
    }
    SaslMessage(String appId, ByteBuf message){
        super(new NettyManagedBuffer(message),true);
        this.appId=appId;
    }

    @Override
    public Type type() {
        return Type.User;
    }

    @Override
    public int encodedLength() {
        return 1+ Encoders.Strings.encodedLength(appId)+4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeByte(TAG_BYTE);
        Encoders.Strings.encode(buf,appId);
        buf.writeInt((int) body().size());
    }

    public static SaslMessage decode(ByteBuf buf){
        if (buf.readByte() != TAG_BYTE) {
            throw new IllegalArgumentException("Expected SaslMessage, received something else" +
                    "(maybe your client does not have SASL enabled?)");
        }
        String appId = Encoders.Strings.decode(buf);
        buf.readInt();
        return new SaslMessage(appId,buf.retain());
    }
}
