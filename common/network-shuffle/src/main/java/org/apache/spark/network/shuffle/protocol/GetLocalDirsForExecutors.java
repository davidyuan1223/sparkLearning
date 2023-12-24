package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;
import java.util.Objects;

public class GetLocalDirsForExecutors extends BlockTransferMessage{
    public final String appId;
    public final String[] execIds;

    public GetLocalDirsForExecutors(String appId, String[] execIds) {
        this.appId = appId;
        this.execIds = execIds;
    }

    @Override
    protected Type type() { return Type.GET_LOCAL_DIRS_FOR_EXECUTORS; }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId) * 41 + Arrays.hashCode(execIds);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("appId", appId)
                .append("execIds", Arrays.toString(execIds))
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof GetLocalDirsForExecutors) {
            GetLocalDirsForExecutors o = (GetLocalDirsForExecutors) other;
            return appId.equals(o.appId) && Arrays.equals(execIds, o.execIds);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId) + Encoders.StringArrays.encodedLength(execIds);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.StringArrays.encode(buf, execIds);
    }

    public static GetLocalDirsForExecutors decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String[] execIds = Encoders.StringArrays.decode(buf);
        return new GetLocalDirsForExecutors(appId, execIds);
    }
}
