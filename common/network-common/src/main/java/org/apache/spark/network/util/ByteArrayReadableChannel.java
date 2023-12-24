package org.apache.spark.network.util;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;

public class ByteArrayReadableChannel implements ReadableByteChannel {
    private ByteBuf data;
    private boolean closed;
    public void feedData(ByteBuf buf)throws ClosedChannelException{
        if (closed) {
            throw new ClosedChannelException();
        }
        data=buf;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (closed) {
            throw new ClosedChannelException();
        }
        int totalRead=0;
        while (data.readableBytes() > 0 && dst.remaining() > 0) {
            int bytesToRead = Math.min(data.readableBytes(), dst.remaining());
            dst.put(data.readSlice(bytesToRead).nioBuffer());
            totalRead+=bytesToRead;
        }
        return totalRead;
    }

    @Override
    public void close() throws IOException {
        closed=true;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }
}
