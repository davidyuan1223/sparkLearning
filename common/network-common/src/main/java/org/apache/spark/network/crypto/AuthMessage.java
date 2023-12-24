package org.apache.spark.network.crypto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

import java.nio.ByteBuffer;

public class AuthMessage implements Encodable {
    private static final byte TAG_BYTE= (byte) 0xFB;
    public final String appId;
    public final byte[] salt;
    public final byte[] ciphertext;

    public AuthMessage(String appId, byte[] salt, byte[] ciphertext) {
        this.appId = appId;
        this.salt = salt;
        this.ciphertext = ciphertext;
    }

    @Override
    public int encodedLength() {
        return 1+ Encoders.Strings.encodedLength(appId)
                +Encoders.ByteArrays.encodedLength(salt)
                +Encoders.ByteArrays.encodedLength(ciphertext);
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeInt(TAG_BYTE);
        Encoders.Strings.encode(buf,appId);
        Encoders.ByteArrays.encode(buf,salt);
        Encoders.ByteArrays.encode(buf,ciphertext);
    }
    public static AuthMessage decodeMessage(ByteBuffer buffer){
        ByteBuf buf = Unpooled.wrappedBuffer(buffer);
        if (buf.readByte() != TAG_BYTE) {
            throw new IllegalArgumentException("Expected ClientChallenge, received something else.");
        }
        return new AuthMessage(
                Encoders.Strings.decode(buf),
                Encoders.ByteArrays.decode(buf),
                Encoders.ByteArrays.decode(buf)
        );
    }
}
