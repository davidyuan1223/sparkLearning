package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

import java.util.Objects;

public class RegisterExecutor extends BlockTransferMessage {
    public final String appId;
    public final String execId;
    public final ExecutorShuffleInfo executorInfo;

    public RegisterExecutor(String appId, String execId, ExecutorShuffleInfo executorInfo) {
        this.appId = appId;
        this.execId = execId;
        this.executorInfo = executorInfo;
    }

    @Override
    protected Type type() {
        return Type.REGISTER_EXECUTOR;
    }

    @Override
    public int hashCode() {
        return Objects.hash(appId, execId, executorInfo);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("appId", appId)
                .append("execId", execId)
                .append("executorInfo", executorInfo)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RegisterExecutor) {
            RegisterExecutor o = (RegisterExecutor) other;
            return Objects.equals(appId, o.appId)
                    && Objects.equals(execId, o.execId)
                    && Objects.equals(executorInfo, o.executorInfo);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                +Encoders.Strings.encodedLength(execId)
                +executorInfo.encodedLength();
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf,appId);
        Encoders.Strings.encode(buf,execId);
        executorInfo.encode(buf);
    }

    public static RegisterExecutor decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        ExecutorShuffleInfo executorShuffleInfo = ExecutorShuffleInfo.decode(buf);
        return new RegisterExecutor(appId, execId, executorShuffleInfo);
    }
}
