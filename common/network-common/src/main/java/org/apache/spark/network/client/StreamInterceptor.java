package org.apache.spark.network.client;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.server.MessageHandler;
import org.apache.spark.network.util.TransportFrameDecoder;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

public class StreamInterceptor<T extends Message> implements TransportFrameDecoder.Interceptor {
    private final MessageHandler<T> handler;
    private final String streamId;
    private final long byteCount;
    private final StreamCallback callback;
    private long bytesRead;

    public StreamInterceptor(MessageHandler<T> handler, String streamId, long byteCount, StreamCallback callback) {
        this.handler = handler;
        this.streamId = streamId;
        this.byteCount = byteCount;
        this.callback = callback;
        this.bytesRead=0;
    }

    @Override
    public void exceptionCaught(Throwable cause) throws Exception {
        deactivateStream();
        callback.onFailure(streamId,cause);
    }

    @Override
    public void channelInactive() throws Exception {
        deactivateStream();
        callback.onFailure(streamId,new ClosedChannelException());
    }

    private void deactivateStream(){
        if (handler instanceof TransportResponseHandler) {
            ((TransportResponseHandler)handler).deactivateStream();
        }
    }

    @Override
    public boolean handle(ByteBuf data) throws Exception {
        int toRead = (int) Math.min(data.readableBytes(),byteCount-bytesRead);
        ByteBuffer nioBuffer = data.readSlice(toRead).nioBuffer();
        int available = nioBuffer.remaining();
        callback.onData(streamId,nioBuffer);
        bytesRead+=available;
        if (bytesRead > byteCount) {
            RuntimeException re=new IllegalArgumentException(String.format(
                    "Read to many bytes? Expected %d, but read %d.",byteCount,bytesRead
            ));
            callback.onFailure(streamId,re);
            deactivateStream();
            throw re;
        } else if (byteCount == bytesRead) {
            deactivateStream();
            callback.onComplete(streamId);
        }
        return byteCount!=bytesRead;
    }
}
