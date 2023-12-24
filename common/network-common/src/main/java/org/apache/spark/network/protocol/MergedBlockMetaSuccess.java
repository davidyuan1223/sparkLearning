package org.apache.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

public class MergedBlockMetaSuccess extends AbstractResponseMessage{
    public final long requestId;
    public final int numChunks;
    public MergedBlockMetaSuccess(
            long requestId,
            int numChunks,
            ManagedBuffer chunkBitmapBuffer
    ){
        super(chunkBitmapBuffer,true);
        this.requestId=requestId;
        this.numChunks=numChunks;
    }

    @Override
    public Type type() {
        return Type.MergedBlockMetaSuccess;
    }
    @Override
    public int hashCode() {
        return Objects.hashCode(requestId, numChunks);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("requestId", requestId).append("numChunks", numChunks).toString();
    }

    @Override
    public int encodedLength() {
        return 8+4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(requestId);
        buf.writeInt(numChunks);
    }

    public int getNumChunks() {
        return numChunks;
    }
    public static MergedBlockMetaSuccess decode(ByteBuf buf){
        long requestId = buf.readLong();
        int numChunks = buf.readInt();
        buf.retain();
        NettyManagedBuffer managedBuffer = new NettyManagedBuffer(buf.duplicate());
        return new MergedBlockMetaSuccess(requestId,numChunks,managedBuffer);
    }

    @Override
    public ResponseMessage createFailureResponse(String error) {
        return new RpcFailure(requestId,error);
    }
}
