package org.apache.spark.network.crypto;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.apache.commons.crypto.stream.CryptoInputStream;
import org.apache.commons.crypto.stream.CryptoOutputStream;
import org.apache.spark.network.util.AbstractFileRegion;
import org.apache.spark.network.util.ByteArrayReadableChannel;
import org.apache.spark.network.util.ByteArrayWritableChannel;

import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Properties;

public class TransportCipher {
    @VisibleForTesting
    static final String ENCRYPTION_HANDLER_NAME="TransportEncryption";
    private static final String DECRYPTION_HANDLER_NAME="TransportDecryption";
    @VisibleForTesting
    static final int STREAM_BUFFER_SIZE=1024*32;
    private final Properties conf;
    private final String cipher;
    private final SecretKeySpec key;
    private final byte[] inIv;
    private final byte[] outIv;

    public TransportCipher(Properties conf, String cipher, SecretKeySpec key, byte[] inIv, byte[] outIv) {
        this.conf = conf;
        this.cipher = cipher;
        this.key = key;
        this.inIv = inIv;
        this.outIv = outIv;
    }
    public String getCipherTransformation(){
        return cipher;
    }
    @VisibleForTesting
    SecretKeySpec getKey(){
        return key;
    }

    public byte[] getInputIv() {
        return inIv;
    }

    public byte[] getOutputIv() {
        return outIv;
    }
    @VisibleForTesting
    CryptoOutputStream createOutputStream(WritableByteChannel ch)throws IOException{
        return new CryptoOutputStream(cipher,conf,ch,key,new IvParameterSpec(outIv));
    }
    @VisibleForTesting
    CryptoInputStream createInputStream(ReadableByteChannel ch)throws IOException{
        return new CryptoInputStream(cipher,conf,ch,key,new IvParameterSpec(inIv));
    }
    public void addToChannel(Channel ch)throws IOException{
        ch.pipeline()
                .addFirst(ENCRYPTION_HANDLER_NAME,new EncryptionHandler(this))
                .addFirst(DECRYPTION_HANDLER_NAME,new DecryptionHandler(this));
    }

    @VisibleForTesting
    static class EncryptionHandler extends ChannelOutboundHandlerAdapter{
        private final ByteArrayWritableChannel byteEncChannel;
        private final CryptoOutputStream cos;
        private final ByteArrayWritableChannel byteRawChannel;
        private boolean isCipherValid;
        EncryptionHandler(TransportCipher cipher)throws IOException{
            byteEncChannel=new ByteArrayWritableChannel(STREAM_BUFFER_SIZE);
            cos=cipher.createOutputStream(byteEncChannel);
            byteRawChannel=new ByteArrayWritableChannel(STREAM_BUFFER_SIZE);
            isCipherValid=true;
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            ctx.write(createEncryptedMessage(msg),promise);
        }
        @VisibleForTesting
        EncryptedMessage createEncryptedMessage(Object msg){
            return new EncryptedMessage(this,cos,msg,byteEncChannel,byteRawChannel);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            try {
                if (isCipherValid) {
                    cos.close();
                }
            }finally {
                super.close(ctx,promise);
            }
        }
        void reportError(){
            this.isCipherValid=false;
        }

        public boolean isCipherValid() {
            return isCipherValid;
        }
    }

    private static class DecryptionHandler extends ChannelInboundHandlerAdapter{
        private final CryptoInputStream cis;
        private final ByteArrayReadableChannel byteChannel;
        private boolean isCipherValid;
        DecryptionHandler(TransportCipher cipher)throws IOException{
            byteChannel=new ByteArrayReadableChannel();
            cis=cipher.createInputStream(byteChannel);
            isCipherValid=true;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf buffer=(ByteBuf) msg;
            try {
                if (!isCipherValid) {
                    throw new IOException("Cipher is in invalid state.");
                }
                byte[] decryptedData = new byte[buffer.readableBytes()];
                byteChannel.feedData(buffer);
                int offset=0;
                while (offset < decryptedData.length) {
                    try {
                        offset+=cis.read(decryptedData,offset,decryptedData.length-offset);
                    }catch (InternalError e){
                        isCipherValid=false;
                        throw e;
                    }
                }
                ctx.fireChannelRead(Unpooled.wrappedBuffer(decryptedData,0,decryptedData.length));
            }finally {
                buffer.release();
            }
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            try {
                if (isCipherValid) {
                    cis.close();
                }
            }finally {
                super.handlerRemoved(ctx);
            }
        }
    }

    @VisibleForTesting
    static class EncryptedMessage extends AbstractFileRegion{
        private final boolean isByteBuf;
        private final ByteBuf buf;
        private final FileRegion region;
        private final CryptoOutputStream cos;
        private final EncryptionHandler handler;
        private final long count;
        private long transferred;
        private final ByteArrayWritableChannel byteEncChannel;
        private final ByteArrayWritableChannel byteRawChannel;
        private ByteBuffer currentEncrypted;


        EncryptedMessage(
                EncryptionHandler handler,
                CryptoOutputStream cos,
                Object msg,
                ByteArrayWritableChannel byteEncChannel,
                ByteArrayWritableChannel byteRawChannel
        ){
            Preconditions.checkArgument(msg instanceof ByteBuf || msg instanceof FileRegion,
                    "Unrecognized message type: %s",msg.getClass().getName());
            this.handler=handler;
            this.isByteBuf=msg instanceof ByteBuf;
            this.buf= isByteBuf? (ByteBuf) msg:null;
            this.region=isByteBuf?null:(FileRegion) msg;
            this.transferred=0;
            this.cos=cos;
            this.byteEncChannel=byteEncChannel;
            this.byteRawChannel=byteRawChannel;
            this.count=isByteBuf?buf.readableBytes():region.count();
        }

        @Override
        public long count() {
            return count;
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
            if (region != null) {
                region.touch(o);
            }
            if (buf != null) {
                buf.touch(o);
            }
            return this;
        }

        @Override
        public EncryptedMessage retain(int increment) {
            super.retain(increment);
            if (region != null) {
                region.retain(increment);
            }
            if (buf != null) {
                buf.retain(increment);
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
            Preconditions.checkArgument(position == transferred, "Invalid position");
            if (transferred==count) {
                return 0;
            }
            long totalBytesWritten=0L;
            do {
                if (currentEncrypted == null) {
                    encryptMore();
                }
                long remaining = currentEncrypted.remaining();
                if (remaining==0) {
                    currentEncrypted=null;
                    byteEncChannel.reset();
                    return totalBytesWritten;
                }
                int bytesWritten = writableByteChannel.write(currentEncrypted);
                totalBytesWritten+=bytesWritten;
                transferred+=bytesWritten;
                if (bytesWritten < remaining) {
                    break;
                }
                currentEncrypted=null;
                byteEncChannel.reset();
            }while (transferred<count);

            return totalBytesWritten;
        }

        private void encryptMore() throws IOException{
            if (!handler.isCipherValid) {
                throw new IOException("Cipher is in invalid state.");
            }
            byteRawChannel.reset();
            if (isByteBuf) {
                int copied=byteRawChannel.write(buf.nioBuffer());
                buf.skipBytes(copied);
            }else {
                region.transferTo(byteRawChannel,region.transferred());
            }
            try{
                cos.write(byteRawChannel.getData(), 0, byteRawChannel.length());
                cos.flush();
            }catch (InternalError e){
                handler.reportError();
                throw e;
            }
            currentEncrypted=ByteBuffer.wrap(byteEncChannel.getData(),0, byteEncChannel.length());
        }

        @Override
        protected void deallocate() {
            byteRawChannel.reset();
            byteEncChannel.reset();
            if (region != null) {
                region.release();
            }
            if (buf != null) {
                buf.release();
            }
        }
    }
}
