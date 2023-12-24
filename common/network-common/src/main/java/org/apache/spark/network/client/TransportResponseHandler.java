package org.apache.spark.network.client;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.network.protocol.*;
import org.apache.spark.network.server.MessageHandler;
import org.apache.spark.network.util.TransportFrameDecoder;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);
    private final Channel channel;
    private final Map<StreamChunkId,ChunkReceivedCallback> outstandingFetches;
    private final Map<Long,BaseResponseCallback> outstandingRpcs;
    private final Queue<Pair<String ,StreamCallback>> streamCallbacks;
    private volatile boolean streamActive;
    private final AtomicLong timeOfLastRequestNs;
    public TransportResponseHandler(Channel channel){
        this.channel=channel;
        this.outstandingFetches=new ConcurrentHashMap<>();
        this.outstandingRpcs=new ConcurrentHashMap<>();
        this.streamCallbacks=new ConcurrentLinkedQueue<>();
        this.timeOfLastRequestNs =new AtomicLong(0);
    }
    public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback){
        updateTimeOfLastRequest();
        outstandingFetches.put(streamChunkId,callback);
    }
    public void removeFetchRequest(StreamChunkId streamChunkId){
        outstandingFetches.remove(streamChunkId);
    }
    public void addRpcRequest(long requestId, BaseResponseCallback callback){
        updateTimeOfLastRequest();
        outstandingRpcs.put(requestId,callback);
    }
    public void removeRpcRequest(long requestId){
        outstandingRpcs.remove(requestId);
    }
    public void addStreamCallback(String streamId, StreamCallback callback){
        updateTimeOfLastRequest();
        streamCallbacks.offer(ImmutablePair.of(streamId,callback));
    }
    @VisibleForTesting
    public void deactivateStream(){
        streamActive=false;
    }
    private void failOutstandingRequests(Throwable cause){
        for (Map.Entry<StreamChunkId, ChunkReceivedCallback> entry : outstandingFetches.entrySet()) {
            try {
                entry.getValue().onFailure(entry.getKey().chunkIndex,cause);
            }catch (Exception e){
                logger.warn("ChunkReceivedCallback,onFailure throws exception",e);
            }
        }
        for (BaseResponseCallback callback : outstandingRpcs.values()) {
            try {
                callback.onFailure(cause);
            }catch (Exception e){
                logger.warn("RpcResponseCallback.onFailure throws exception", e);
            }
        }
        for (Pair<String, StreamCallback> entry : streamCallbacks) {
            try {
                entry.getValue().onFailure(entry.getKey(),cause);
            }catch (Exception e){
                logger.warn("StreamCallback.onFailure throws exception", e);
            }
        }
        outstandingFetches.clear();
        outstandingRpcs.clear();
        streamCallbacks.clear();
    }

    @Override
    public void channelActive() {

    }

    @Override
    public void channelInactive() {
        if (hasOutstandingRequests()) {
            String remoteAddress = getRemoteAddress(channel);
            logger.error("Still have {} requests outstanding when connection fron {} is closed",
                    numOutstandingRequests(), remoteAddress);
            failOutstandingRequests(new IOException("Connection from "+remoteAddress+" closed"));
        }
    }

    @Override
    public void exceptionCaught(Throwable cause) {
        if (hasOutstandingRequests()) {
            String remoteAddress = getRemoteAddress(channel);
            logger.error("Still have {} requests outstanding when connection fron {} is closed",
                    numOutstandingRequests(), remoteAddress);
            failOutstandingRequests(new IOException("Connection from "+remoteAddress+" closed"));
        }
    }

    @Override
    public void handle(ResponseMessage message) throws Exception {
        if (message instanceof ChunkFetchSuccess) {
            ChunkFetchSuccess resp=(ChunkFetchSuccess) message;
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            if (listener == null) {
                logger.warn("Ignoring response for block {} from {} since it is not outstanding",
                        resp.streamChunkId, getRemoteAddress(channel));
                resp.body().release();
            }else {
                outstandingFetches.remove(resp.streamChunkId);
                listener.onSuccess(resp.streamChunkId.chunkIndex, resp.body());
                resp.body().release();
            }
        } else if (message instanceof ChunkFetchFailure) {
            ChunkFetchFailure resp = (ChunkFetchFailure) message;
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            if (listener == null) {
                logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
                        resp.streamChunkId,getRemoteAddress(channel),resp.errorString);
            }else {
                outstandingFetches.remove(resp.streamChunkId);
                listener.onFailure(resp.streamChunkId.chunkIndex, new ChunkFetchFailureException(
                        "Failure while fetching "+resp.streamChunkId+": "+resp.errorString
                ));
            }
        } else if (message instanceof RpcResponse) {
            RpcResponse resp = (RpcResponse) message;
            RpcResponseCallback listener = (RpcResponseCallback) outstandingRpcs.get(resp.requestId);
            if (listener == null) {
                logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
                        resp.requestId, getRemoteAddress(channel),resp.body().size());
                resp.body().release();
            }else {
                outstandingRpcs.remove(resp.requestId);
                try {
                    listener.onSuccess(resp.body().nioByteBuffer());
                }finally {
                    resp.body().release();
                }
            }
        }else if (message instanceof RpcFailure){
            RpcFailure resp = (RpcFailure) message;
            RpcResponseCallback listener = (RpcResponseCallback) outstandingRpcs.get(resp.requestId);
            if (listener == null) {
                logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
                        resp.requestId, getRemoteAddress(channel),resp.errorString);
            }else {
                outstandingRpcs.remove(resp.requestId);
                listener.onFailure(new RuntimeException(resp.errorString));
            }
        } else if (message instanceof MergedBlockMetaSuccess) {
            MergedBlockMetaSuccess resp = (MergedBlockMetaSuccess) message;
            try {
                MergedBlockMetaResponseCallback listener =
                        (MergedBlockMetaResponseCallback) outstandingRpcs.get(resp.requestId);
                if (listener == null) {
                    logger.warn("Ignoring response for MergedBlockMetaRequest {} from {} ({} bytes) since it is not outstanding",
                            resp.requestId,getRemoteAddress(channel),resp.body().size());
                }else {
                    outstandingRpcs.remove(resp.requestId);
                    listener.onSuccess(resp.getNumChunks(), resp.body());
                }
            }finally {
                resp.body().release();
            }
        } else if (message instanceof StreamResponse) {
            StreamResponse resp = (StreamResponse) message;
            Pair<String, StreamCallback> entry = streamCallbacks.poll();
            if (entry != null) {
                StreamCallback callback = entry.getValue();
                if (resp.byteCount > 0) {
                    StreamInterceptor<ResponseMessage> interceptor = new StreamInterceptor<>(
                            this, resp.streamId, resp.byteCount, callback
                    );
                    try {
                        TransportFrameDecoder frameDecoder= (TransportFrameDecoder) channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
                        frameDecoder.setInterceptor(interceptor);
                        streamActive=true;
                    }catch (Exception e){
                        logger.error("Error installing stream handler.",e);
                        deactivateStream();
                    }
                }else {
                    try {
                        callback.onComplete(resp.streamId);
                    }catch (Exception e){
                        logger.warn("Error in stream handler onComplete().",e);
                    }
                }
            }else {
                logger.warn("Could not find callback for StreamResponse.");
            }
        } else if (message instanceof StreamFailure) {
            StreamFailure resp=(StreamFailure) message;
            Pair<String, StreamCallback> entry = streamCallbacks.poll();
            if (entry != null) {
                StreamCallback callback = entry.getValue();
                try {
                    callback.onFailure(resp.streamId,new RuntimeException(resp.error));
                }catch (IOException e){
                    logger.warn("Error in stream failure handler.",e);
                }
            }else {
                logger.warn("Stream failure with unknown callback: {}",resp.error);
            }
        }else {
            throw new IllegalStateException("Unknown response type: "+message.type() );
        }
    }
    public int numOutstandingRequests(){
        return outstandingFetches.size()+outstandingRpcs.size()+streamCallbacks.size()+(streamActive?1:0);
    }
    public Boolean hasOutstandingRequests(){
        return streamActive||!outstandingFetches.isEmpty()||!outstandingRpcs.isEmpty()||!streamCallbacks.isEmpty();
    }

    public long getTimeOfLastRequestNs() {
        return timeOfLastRequestNs.get();
    }
    public void updateTimeOfLastRequest(){
        timeOfLastRequestNs.set(System.nanoTime());
    }

}
