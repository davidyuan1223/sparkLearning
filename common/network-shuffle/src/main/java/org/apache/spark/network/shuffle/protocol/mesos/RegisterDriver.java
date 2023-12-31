package org.apache.spark.network.shuffle.protocol.mesos;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;

public class RegisterDriver extends BlockTransferMessage {
    private final String appId;
    private final long heartbeatTimeoutMs;
    public RegisterDriver(String appId, long heartbeatTimeoutMs) {
        this.appId = appId;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public String getAppId() { return appId; }

    public long getHeartbeatTimeoutMs() { return heartbeatTimeoutMs; }

    @Override
    protected Type type() { return Type.REGISTER_DRIVER; }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId) + Long.SIZE / Byte.SIZE;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        buf.writeLong(heartbeatTimeoutMs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, heartbeatTimeoutMs);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RegisterDriver)) {
            return false;
        }
        return Objects.equal(appId, ((RegisterDriver) o).appId);
    }

    public static RegisterDriver decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        long heartbeatTimeout = buf.readLong();
        return new RegisterDriver(appId, heartbeatTimeout);
    }
}
