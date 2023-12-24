package org.apache.spark.network.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.LinkedList;

public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {
    public static final String HANDLER_NAME="frameDecoder";
    private static final int LENGTH_SIZE=8;
    private static final int MAX_FRAME_SIZE=Integer.MAX_VALUE;
    private static final int UNKNOWN_FRAME_SIZE=-1;
    private static final long CONSOLIDATE_THRESHOLD=20*1024*1024;
    private final LinkedList<ByteBuf> buffers=new LinkedList<>();
    private final ByteBuf frameLenBuf = Unpooled.buffer(LENGTH_SIZE,LENGTH_SIZE);
    private final long consolidateThreshold;
    private CompositeByteBuf frameBuf=null;
    private long consolidateFrameBufSize=0;
    private int consolidateNumComponents=0;
    private long totalSize=0;
    private long nextFrameSize=UNKNOWN_FRAME_SIZE;
    private int frameRemainingBytes=UNKNOWN_FRAME_SIZE;
    private volatile Interceptor interceptor;
    public TransportFrameDecoder(){
        this(CONSOLIDATE_THRESHOLD);
    }
    @VisibleForTesting
    TransportFrameDecoder(long consolidateThreshold){
        this.consolidateThreshold=consolidateThreshold;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf in = (ByteBuf) msg;
        buffers.add(in);
        totalSize+=in.readableBytes();
        while (!buffers.isEmpty()) {
            if (interceptor != null) {
                ByteBuf first = buffers.getFirst();
                int available = first.readableBytes();
                if (feedInterceptor(first)) {
                    assert !first.isReadable():"Interceptor still active buf buffer has data.";
                }
                int read = available - first.readableBytes();
                if (read == available) {
                    buffers.removeFirst().release();
                }
                totalSize-=read;
            }else {
                ByteBuf frame=decodeNext();
                if (frame == null) {
                    break;
                }
                ctx.fireChannelRead(frame);
            }
        }
    }

    private long decodeFrameSize(){
        if (nextFrameSize != UNKNOWN_FRAME_SIZE || totalSize < LENGTH_SIZE) {
            return nextFrameSize;
        }
        ByteBuf first = buffers.getFirst();
        if (first.readableBytes() >= LENGTH_SIZE) {
            nextFrameSize=first.readLong()-LENGTH_SIZE;
            totalSize-=LENGTH_SIZE;
            if (!first.isReadable()) {
                buffers.removeFirst().release();
            }
            return nextFrameSize;
        }
        while (frameLenBuf.readableBytes() < LENGTH_SIZE) {
            ByteBuf next = buffers.getFirst();
            int toRead = Math.min(next.readableBytes(), LENGTH_SIZE - frameLenBuf.readableBytes());
            frameLenBuf.writeBytes(next,toRead);
            if (!next.isReadable()) {
                buffers.removeFirst().release();
            }
        }
        nextFrameSize=frameLenBuf.readLong()-LENGTH_SIZE;
        totalSize-=LENGTH_SIZE;
        frameLenBuf.clear();
        return nextFrameSize;
    }

    private ByteBuf decodeNext(){
        long frameSize = decodeFrameSize();
        if (frameSize == UNKNOWN_FRAME_SIZE) {
            return null;
        }
        if (frameBuf == null) {
            Preconditions.checkArgument(frameSize<MAX_FRAME_SIZE,"Too large frame: %s",frameSize);
            Preconditions.checkArgument(frameSize>0,"Frame length should be positive: %s",frameSize);
            frameRemainingBytes=(int) frameSize;
            if (buffers.isEmpty()) {
                return null;
            }
            if (buffers.getFirst().readableBytes() >= frameRemainingBytes) {
                frameBuf=null;
                nextFrameSize=UNKNOWN_FRAME_SIZE;
                return nextBufferForFrame(frameRemainingBytes);
            }
            frameBuf=buffers.getFirst().alloc().compositeBuffer(Integer.MAX_VALUE);
        }
        while (frameRemainingBytes > 0 && !buffers.isEmpty()) {
            ByteBuf next=nextBufferForFrame(frameRemainingBytes);
            frameRemainingBytes-=next.readableBytes();
            frameBuf.addComponent(true,next);
        }
        if (frameBuf.capacity() - consolidateFrameBufSize > consolidateThreshold) {
            int newNumComponents = frameBuf.numComponents() - consolidateNumComponents;
            frameBuf.consolidate(consolidateNumComponents,newNumComponents);
            consolidateFrameBufSize=frameBuf.capacity();
            consolidateNumComponents=frameBuf.numComponents();
        }
        if (frameRemainingBytes>0) {
            return null;
        }
        return consumeCurrentFrameBuf();
    }

    private ByteBuf consumeCurrentFrameBuf(){
        ByteBuf frame = this.frameBuf;
        frameBuf=null;
        consolidateFrameBufSize=0;
        consolidateNumComponents=0;
        nextFrameSize=UNKNOWN_FRAME_SIZE;
        return frame;
    }
    private ByteBuf nextBufferForFrame(int bytesToRead){
        ByteBuf buf = buffers.getFirst();
        ByteBuf frame;
        if (buf.readableBytes() > bytesToRead) {
            frame=buf.retain().readSlice(bytesToRead);
            totalSize-=bytesToRead;
        }else {
            frame=null;
            buffers.removeFirst();
            totalSize-=frame.readableBytes();
        }
        return frame;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (interceptor != null) {
            interceptor.channelInactive();
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (interceptor != null) {
            interceptor.exceptionCaught(cause);
        }
        super.exceptionCaught(ctx,cause);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        for (ByteBuf b : buffers) {
            b.release();
        }
        buffers.clear();
        frameLenBuf.release();
        ByteBuf frame = consumeCurrentFrameBuf();
        if (frame != null) {
            frame.release();
        }
        super.handlerRemoved(ctx);
    }

    public void setInterceptor(Interceptor interceptor) {
        Preconditions.checkState(this.interceptor==null,"Already have an interceptor");
        this.interceptor = interceptor;
    }
    private boolean feedInterceptor(ByteBuf buf)throws Exception{
        if (interceptor != null && !interceptor.handle(buf)) {
            interceptor=null;
        }
        return interceptor!=null;
    }

    public interface Interceptor {
        boolean handle(ByteBuf data)throws Exception;
        void exceptionCaught(Throwable cause)throws Exception;
        void channelInactive()throws Exception;
    }
}
