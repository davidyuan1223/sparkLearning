package org.apache.spark.network.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.network.util.NettyUtils.*;


public class TransportClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);
    private final Channel channel;
    private final TransportResponseHandler handler;
    @Nullable
    private String clientId;
    private volatile boolean timeOut;

    public TransportClient(Channel channel, TransportResponseHandler handler) {
        this.channel = Preconditions.checkNotNull(channel);
        this.handler = Preconditions.checkNotNull(handler);
        this.timeOut=false;
    }

    public Channel getChannel() {
        return channel;
    }
    public boolean isActive(){
        return !timeOut&&(channel.isOpen()||channel.isActive());
    }
    public SocketAddress getSocketAddress(){
        return channel.remoteAddress();
    }

    @Nullable
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String id) {
        Preconditions.checkState(clientId==null,"Client ID has already been set.");
        this.clientId = id;
    }

    public void fetchChunk(long streamId,int chunkIndex,ChunkReceivedCallback callback){
        if (logger.isDebugEnabled()) {
            logger.debug("Sending fetch chunk request {} to {}", chunkIndex,getRemoteAddress(channel));
        }
        StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
        StdChannelListener listener = new StdChannelListener(streamChunkId) {
            @Override
            void handleFailure(String errorMsg, Throwable cause) throws Exception {
                handler.removeFetchRequest(streamChunkId);
                callback.onFailure(chunkIndex, new IOException(errorMsg, cause));
            }
        };
        handler.addFetchRequest(streamChunkId,callback);
        channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(listener);
    }

    public void stream(String streamId, StreamCallback callback){
        StdChannelListener listener = new StdChannelListener(streamId) {
            @Override
            void handleFailure(String errorMsg, Throwable cause) throws Exception {
                callback.onFailure(streamId, new IOException(errorMsg, cause));
            }
        };
        if (logger.isDebugEnabled()) {
            logger.debug("Sending stream request for {} to {}",streamId,getRemoteAddress(channel));
        }
        synchronized (this){
            handler.addStreamCallback(streamId,callback);
            channel.writeAndFlush(new StreamRequest(streamId)).addListener(listener);
        }
    }

    public long sendRpc(ByteBuffer message, RpcResponseCallback callback){
        if (logger.isTraceEnabled()) {
            logger.trace("Sending RPC to {}",getRemoteAddress(channel));
        }
        long requestId=requestId();
        handler.addRpcRequest(requestId,callback);
        RpcChannelListener listener = new RpcChannelListener(requestId, callback);
        channel.writeAndFlush(new RpcRequest(requestId,new NioManagedBuffer(message))).addListener(listener);
        return requestId;
    }

    public void sendMergedBlockMetaReq(String appId, int shuffleId,int shuffleMergedId,int reduceId,MergedBlockMetaResponseCallback callback){
        long requestId=requestId();
        if (logger.isTraceEnabled()) {
            logger.trace("Sending RPC {} to fetch merged block meta to {}",requestId,getRemoteAddress(channel));
        }
        handler.addRpcRequest(requestId,callback);
        RpcChannelListener listener = new RpcChannelListener(requestId, callback);
        channel.writeAndFlush(new MergedBlockMetaRequest(requestId,appId,shuffleId,shuffleMergedId,reduceId)).addListener(listener);
    }

    public long uploadStream(ManagedBuffer meta,ManagedBuffer data,RpcResponseCallback callback){
        if (logger.isTraceEnabled()) {
            logger.trace("Sending RPC to {}",getRemoteAddress(channel));
        }
        long requestId=requestId();
        handler.addRpcRequest(requestId,callback);
        RpcChannelListener listener = new RpcChannelListener(requestId, callback);
        channel.writeAndFlush(new UploadStream(requestId,meta,data)).addListener(listener);
        return requestId;
    }

    public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs){
        final SettableFuture<ByteBuffer> result = SettableFuture.create();
        sendRpc(message, new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                try {
                    ByteBuffer copy = ByteBuffer.allocate(response.remaining());
                    copy.put(response);
                    copy.flip();
                    result.set(copy);
                }catch (Throwable t){
                    logger.warn("Error in responding RPC callback",t);
                    result.setException(t);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });
        try {
            return result.get(timeoutMs, TimeUnit.MILLISECONDS);
        }catch (ExecutionException e){
            throw Throwables.propagate(e.getCause());
        }catch (Exception e){
            throw Throwables.propagate(e);
        }
    }

    public void send(ByteBuffer message){
        channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
    }

    public void removeRpcRequest(long requestId){
        handler.removeRpcRequest(requestId);
    }
    public void timeOut(){
        this.timeOut=true;
    }
    @VisibleForTesting
    public TransportResponseHandler getHandler(){
        return handler;
    }

    @Override
    public void close() throws IOException {
        channel.close().awaitUninterruptibly(10,TimeUnit.SECONDS);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("remoteAddress", channel.remoteAddress())
                .append("clientId", clientId)
                .append("isActive", isActive())
                .toString();
    }

    private static long requestId() {
        return Math.abs(UUID.randomUUID().getLeastSignificantBits());
    }
    private class StdChannelListener implements GenericFutureListener<Future<? super Void>> {
        final long startTime;
        final Object requestId;

        public StdChannelListener(Object requestId) {
            this.startTime=System.currentTimeMillis();
            this.requestId = requestId;
        }

        @Override
        public void operationComplete(Future<? super Void> future) throws Exception {
            if (future.isSuccess()) {
                if (logger.isTraceEnabled()) {
                    long timeTaken = System.currentTimeMillis() - startTime;
                    logger.trace("Sending request {} to {} took {} ms",requestId,getRemoteAddress(channel),timeTaken);
                }
            }else {
                String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId, getRemoteAddress(channel), future.cause());
                logger.error(errorMsg,future.cause());
                channel.close();
                try {
                    handleFailure(errorMsg,future.cause());
                }catch (Exception e){
                    logger.error("Uncaught exception in RPC response callback handler!",e);
                }
            }
        }
        void handleFailure(String errorMsg, Throwable cause)throws Exception{}
    }
    private class RpcChannelListener extends StdChannelListener{
        final long rpcRequestId;
        final BaseResponseCallback callback;

        RpcChannelListener(long requestId, BaseResponseCallback callback) {
            super("RPC"+requestId);
            this.rpcRequestId = requestId;
            this.callback = callback;
        }


        @Override
        void handleFailure(String errorMsg, Throwable cause) throws Exception {
            handler.removeRpcRequest(rpcRequestId);
            callback.onFailure(new IOException(errorMsg,cause));
        }
    }
}

