package org.apache.spark.network.sasl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.spark.network.util.AbstractFileRegion;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.NettyUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public class SaslEncryption {
    @VisibleForTesting
    static final String ENCRYPTION_HANDLER_NAME="saslEncryption";
    static void addToChannel(Channel channel,SaslEncryptionBackend backend,int maxOutboundBlockSize){
        channel.pipeline()
                .addFirst(ENCRYPTION_HANDLER_NAME,new EncryptionHandler(maxOutboundBlockSize,backend))
                .addFirst("saslDecryption",new DecryptionHandler(backend))
                .addFirst("saslFrameDecoder", NettyUtils.createFrameDecoder());
    }

    private static class EncryptionHandler extends ChannelOutboundHandlerAdapter{
        private final int maxOutboundBlockSize;
        private final SaslEncryptionBackend backend;

        public EncryptionHandler(int maxOutboundBlockSize, SaslEncryptionBackend backend) {
            this.maxOutboundBlockSize = maxOutboundBlockSize;
            this.backend = backend;
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            ctx.write(new EncryptedMessage(backend,msg,maxOutboundBlockSize),promise);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            try {
                backend.dispose();
            }finally {
                super.handlerRemoved(ctx);
            }
        }
    }

    private static class DecryptionHandler extends MessageToMessageDecoder<ByteBuf> {
        private final SaslEncryptionBackend backend;
        DecryptionHandler(SaslEncryptionBackend backend){
            this.backend=backend;
        }

        @Override
        protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf buf, List<Object> list) throws Exception {
            byte[] data;
            int offset;
            int length=buf.readableBytes();
            if (buf.hasArray()) {
                data=buf.array();
                offset=buf.arrayOffset();
                buf.skipBytes(length);
            }else {
                data=new byte[length];
                buf.readBytes(data);
                offset=0;
            }
            list.add(Unpooled.wrappedBuffer(backend.unwrap(data,offset,length)));
        }
    }

    @VisibleForTesting
    static class EncryptedMessage extends AbstractFileRegion{
        private final SaslEncryptionBackend backend;
        private final boolean isByteBuf;
        private final ByteBuf buf;
        private final FileRegion region;
        private final int maxOutboundBlockSize;
        private ByteArrayWritableChannel byteChannel;
        private ByteBuf currentHeader;
        private ByteBuffer currentChunk;
        private long currentChunkSize;
        private long currentReportedBytes;
        private long unencryptedChunkSize;
        private long transferred;
        EncryptedMessage(SaslEncryptionBackend backend,Object msg,int maxOutboundBlockSize){
            Preconditions.checkArgument(msg instanceof ByteBuf || msg instanceof FileRegion,
                    "Unrecognized message type: %s",msg.getClass().getName());
            this.backend=backend;
            this.isByteBuf=msg instanceof ByteBuf;
            this.buf=isByteBuf?(ByteBuf) msg:null;
            this.region=isByteBuf?null:(FileRegion)msg;
            this.maxOutboundBlockSize=maxOutboundBlockSize;
        }

        @Override
        public long count() {
            return isByteBuf?buf.readableBytes():region.count();
        }

        @Override
        public long position() {
            return 0;
        }


        @Override
        public long transferred() {
            return transferred;
        }

        @Override
        public EncryptedMessage touch(Object o) {
            super.touch(o);
            if (buf != null) {
                buf.touch(o);
            }
            if (region != null) {
                region.touch(o);
            }
            return this;
        }

        @Override
        public EncryptedMessage retain(int increment) {
            super.retain(increment);
            if (buf != null) {
                buf.retain(increment);
            }
            if (region != null) {
                region.retain(increment);
            }
            return this;
        }

        @Override
        public boolean release(int decrement) {
            if (region != null) {
                region.release(decrement);
            }
            if (buf != null) {
                buf.release(decrement);
            }
            return super.release(decrement);
        }

        @Override
        public long transferTo(WritableByteChannel writableByteChannel, long position) throws IOException {
            Preconditions.checkArgument(position==transferred,"Invalid position.");
            long reportedWritten=0L;
            long actuallyWritten=0L;
            do {
                if (currentChunk == null) {
                    nextChunk();
                }
                if (currentHeader.readableBytes()>0) {
                    int bytesWritten = writableByteChannel.write(currentHeader.nioBuffer());
                    currentHeader.skipBytes(bytesWritten);
                    actuallyWritten+=bytesWritten;
                    if (currentHeader.readableBytes()>0) {
                        break;
                    }
                }
                actuallyWritten+=writableByteChannel.write(currentChunk);
                if (!currentChunk.hasRemaining()) {
                    long chunkBytesRemaining = unencryptedChunkSize - currentReportedBytes;
                    reportedWritten+=chunkBytesRemaining;
                    transferred+=chunkBytesRemaining;
                    currentHeader.release();
                    currentHeader=null;
                    currentChunk=null;
                    currentChunkSize=0;
                }
            }while (currentChunk==null && transferred()+reportedWritten<count());
            if (reportedWritten!=0L) {
                return reportedWritten;
            }
            if (actuallyWritten > 0 && currentReportedBytes < currentChunkSize - 1) {
                transferred+=1L;
                currentReportedBytes+=1L;
                return 1L;
            }
            return 0L;
        }
        private void nextChunk()throws IOException{
            if (byteChannel == null) {
                byteChannel=new ByteArrayWritableChannel(maxOutboundBlockSize);
            }
            byteChannel.reset();
            if (isByteBuf) {
                int copied = byteChannel.write(buf.nioBuffer());
                buf.skipBytes(copied);
            }else {
                region.transferTo(byteChannel,region.transferred());
            }
            byte[] encrypted = backend.wrap(byteChannel.getData(), 0, byteChannel.length());
            this.currentChunk=ByteBuffer.wrap(encrypted);
            this.currentChunkSize=encrypted.length;
            this.currentHeader= Unpooled.copyLong(8+currentChunkSize);
            this.unencryptedChunkSize=byteChannel.length();
        }

        @Override
        protected void deallocate() {
            if (currentHeader != null) {
                currentHeader.release();
            }
            if (buf != null) {
                buf.release();
            }
            if (region != null) {
                region.release();
            }
        }
    }
}
