package org.apache.spark.network.protocol;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import io.netty.util.ReferenceCountUtil;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.util.AbstractFileRegion;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class MessageWithHeader extends AbstractFileRegion {
    @Nullable
    private final ManagedBuffer managedBuffer;
    private final ByteBuf header;
    private final int headerLength;
    private final Object body;
    private final long bodyLength;
    private long totalBytesTransferred;
    private static final int NIO_BUFFER_LIMIT=256*1024;
    MessageWithHeader(
            @Nullable ManagedBuffer managedBuffer,
            ByteBuf header,
            Object body,
            long bodyLength
    ){
        Preconditions.checkArgument(body instanceof ByteBuf || body instanceof FileRegion,
                "Body must be a ByteBuf or a FileRegion");
        this.managedBuffer=managedBuffer;
        this.header=header;
        this.headerLength=header.readableBytes();
        this.body=body;
        this.bodyLength=bodyLength;
    }

    @Override
    public long count() {
        return headerLength+bodyLength;
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public long transferred() {
        return totalBytesTransferred;
    }

    @Override
    public long transferTo(WritableByteChannel writableByteChannel, long position) throws IOException {
        Preconditions.checkArgument(position==totalBytesTransferred,"Invalid position");
        long writtenHeader=0;
        if (header.readableBytes()>0) {
            writtenHeader=copyByteBuf(header,writableByteChannel);
            totalBytesTransferred+=writtenHeader;
            if (header.readableBytes()>0) {
                return writtenHeader;
            }
        }
        long writtenBody=0;
        if (body instanceof FileRegion) {
            writtenBody=((FileRegion)body).transferTo(writableByteChannel,totalBytesTransferred-headerLength);
        } else if (body instanceof ByteBuf) {
            writtenBody=copyByteBuf((ByteBuf) body,writableByteChannel);
        }
        totalBytesTransferred+=writtenBody;
        return writtenHeader+writtenBody;
    }

    private int copyByteBuf(ByteBuf buf, WritableByteChannel target) throws IOException {
        int length = Math.min(buf.readableBytes(), NIO_BUFFER_LIMIT);
        int written=0;
        if (buf.nioBufferCount() == 1) {
            ByteBuffer buffer = buf.nioBuffer(buf.readerIndex(), length);
            written=target.write(buffer);
        }else {
            ByteBuffer[] buffers = buf.nioBuffers(buf.readerIndex(), length);
            for (ByteBuffer buffer : buffers) {
                int remaining = buffer.remaining();
                int w = target.write(buffer);
                written+=w;
                if (w < remaining) {
                    break;
                }
            }
        }
        buf.skipBytes(written);
        return written;
    }

    @Override
    protected void deallocate() {
        header.release();
        ReferenceCountUtil.release(body);
        if (managedBuffer != null) {
            managedBuffer.release();
        }
    }

    @Override
    public MessageWithHeader touch(Object o) {
        super.touch(o);
        header.touch(o);
        ReferenceCountUtil.touch(body,o);
        return this;
    }

    @Override
    public MessageWithHeader retain(int increment) {
        super.retain(increment);
        header.retain(increment);
        ReferenceCountUtil.retain(body,increment);
        if (managedBuffer != null) {
            for (int i = 0; i < increment; i++) {
                managedBuffer.retain();
            }
        }
        return this;
    }

    @Override
    public boolean release(int decrement) {
        header.release(decrement);
        ReferenceCountUtil.release(body,decrement);
        if (managedBuffer != null) {
            for (int i = 0; i < decrement; i++) {
                managedBuffer.release();
            }
        }
        return super.release(decrement);
    }
    @Override
    public String toString() {
        return "MessageWithHeader [headerLength: " + headerLength + ", bodyLength: " + bodyLength + "]";
    }
}
