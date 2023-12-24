package org.apache.spark.network.server;

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.*;
import org.apache.spark.network.protocol.*;
import org.apache.spark.network.util.TransportFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import static org.apache.spark.network.util.NettyUtils.*;

public class TransportRequestHandler extends MessageHandler<RequestMessage> {
    private static final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);
    private final Channel channel;
    private final TransportClient reverseClient;
    private final RpcHandler rpcHandler;
    private final StreamManager streamManager;
    private final long maxChunksBeingTransferred;
    private final ChunkFetchRequestHandler chunkFetchRequestHandler;

    public TransportRequestHandler(Channel channel, TransportClient client, RpcHandler rpcHandler, long maxChunksBeingTransferred, ChunkFetchRequestHandler chunkFetchRequestHandler) {
        this.channel = channel;
        this.reverseClient = client;
        this.rpcHandler = rpcHandler;
        this.streamManager = rpcHandler.getStreamManager();
        this.maxChunksBeingTransferred = maxChunksBeingTransferred;
        this.chunkFetchRequestHandler = chunkFetchRequestHandler;
    }

    @Override
    public void exceptionCaught(Throwable cause) {
        rpcHandler.exceptionCaught(cause,reverseClient);
    }

    @Override
    public void channelActive() {
        rpcHandler.channelActive(reverseClient);
    }

    @Override
    public void channelInactive() {
        if (streamManager != null) {
            try {
                streamManager.connectionTerminated(channel);
            }catch (RuntimeException e){
                logger.error("StreamManager connectionTerminated() callback failed.",e);
            }
        }
        rpcHandler.channelInActive(reverseClient);
    }

    @Override
    public void handle(RequestMessage message) throws Exception {
        if (message instanceof ChunkFetchRequest) {
            chunkFetchRequestHandler.processFetchRequest(channel,(ChunkFetchRequest) message);
        } else if (message instanceof RpcRequest) {
            processRpcRequest((RpcRequest)message);
        } else if (message instanceof OneWayMessage) {
            processOneWayMessage((OneWayMessage)message);
        }else if (message instanceof StreamRequest) {
            processStreamRequest((StreamRequest) message);
        }else if (message instanceof UploadStream){
            processStreamUpload((UploadStream)message);
        }else if (message instanceof MergedBlockMetaRequest){
            processMergedBlockMetaRequest((MergedBlockMetaRequest)message);
        }else {
            throw new IllegalArgumentException("Unknown request type: "+message);
        }
    }

    private void processStreamRequest(final StreamRequest request){
        if (logger.isTraceEnabled()) {
            logger.trace("Received req from {} to fetch stream {}", getRemoteAddress(channel),request.streamId);
        }
        if (maxChunksBeingTransferred < Long.MAX_VALUE) {
            long chunksBeingTransferred = streamManager.chunksBeingTransferred();
            if (chunksBeingTransferred >= maxChunksBeingTransferred) {
                logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
                        chunksBeingTransferred,maxChunksBeingTransferred);
                channel.close();
                return;
            }
        }
        ManagedBuffer buf;
        try{
            buf=streamManager.openStream(request.streamId);
        }catch (Exception e){
            logger.error(String.format(
                    "Error opening stream %s for request from %s",request.streamId,getRemoteAddress(channel)
            ),e);
            respond(new StreamFailure(request.streamId, Throwables.getStackTraceAsString(e)));
            return;
        }
        if (buf != null) {
            streamManager.streamSent(request.streamId);
            respond(new StreamResponse(request.streamId,buf.size(),buf)).addListener(future -> {
                streamManager.streamSent(request.streamId);
            });
        }else {
            respond(new StreamFailure(request.streamId, String.format("Stream '%s' was not found.",request.streamId)));
        }
    }

    private void processRpcRequest(final RpcRequest request){
        try {
            rpcHandler.receive(reverseClient, request.body().nioByteBuffer(), new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    respond(new RpcResponse(request.requestId,new NioManagedBuffer(response)));
                }

                @Override
                public void onFailure(Throwable e) {
                    respond(new RpcFailure(request.requestId,Throwables.getStackTraceAsString(e)));
                }
            });
        }catch (Exception e){
            logger.error("Error while invoking RpcHandler#receive() on RPC id "+request.requestId,e);
            respond(new RpcFailure(request.requestId,Throwables.getStackTraceAsString(e)));
        }finally {
            request.body().release();
        }
    }

    private void processStreamUpload(final UploadStream request){
        assert (request.body()!=null);
        try {
            RpcResponseCallback callback = new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    respond(new RpcResponse(request.requestId, new NioManagedBuffer(response)));
                }

                @Override
                public void onFailure(Throwable e) {
                    respond(new RpcFailure(request.requestId, Throwables.getStackTraceAsString(e)));
                }
            };
            TransportFrameDecoder frameDecoder=(TransportFrameDecoder) channel.pipeline()
                    .get(TransportFrameDecoder.HANDLER_NAME);
            ByteBuffer meta = request.meta.nioByteBuffer();
            StreamCallbackWithID streamHandler = rpcHandler.receiveStream(reverseClient, meta, callback);
            if (streamHandler == null) {
                throw new NullPointerException("rpcHandler returned a null streamHandler");
            }
            StreamCallbackWithID wrappedCallback = new StreamCallbackWithID() {
                @Override
                public void onData(String streamId, ByteBuffer buf) throws IOException {
                    streamHandler.onData(streamId, buf);
                }

                @Override
                public void onComplete(String streamId) throws IOException {
                    try {
                        streamHandler.onComplete(streamId);
                        callback.onSuccess(streamHandler.getCompletionResponse());
                    } catch (BlockPushNonFatalFailure ex) {
                        callback.onSuccess(ex.getResponse());
                        streamHandler.onFailure(streamId, ex);
                    } catch (Exception ex) {
                        IOException ioExc = new IOException("Failure post-processing complete stream;" +
                                " failing this rpc and leaving channel active", ex);
                        callback.onFailure(ioExc);
                        streamHandler.onFailure(streamId, ioExc);
                    }
                }

                @Override
                public void onFailure(String streamId, Throwable cause) throws IOException {
                    callback.onFailure(new IOException("Destination failed while reading stream",cause));
                    streamHandler.onFailure(streamId,cause);
                }

                @Override
                public String getID() {
                    return streamHandler.getID();
                }
            };
            if (request.bodyByteCount > 0) {
                StreamInterceptor<RequestMessage> interceptor = new StreamInterceptor<>(
                        this, wrappedCallback.getID(), request.bodyByteCount, wrappedCallback
                );
                frameDecoder.setInterceptor(interceptor);
            }else {
                wrappedCallback.onComplete(wrappedCallback.getID());
            }
        }catch (Exception e){
            if (e instanceof BlockPushNonFatalFailure) {
                respond(new RpcResponse(request.requestId,new NioManagedBuffer(((BlockPushNonFatalFailure)e).getResponse())));
            }else {
                logger.error("Error while invoking RpcHandler#receive() on RPC id "+request.requestId,e);
                respond(new RpcFailure(request.requestId,Throwables.getStackTraceAsString(e)));
            }
            channel.pipeline().fireExceptionCaught(e);
        }finally {
            request.meta.release();
        }
    }

    private void processOneWayMessage(OneWayMessage request){
        try {
            rpcHandler.receive(reverseClient,request.body().nioByteBuffer());
        }catch (Exception e){
            logger.error("Error while invoking RpcHandler#receive() for one-way message.",e);
        }finally {
            request.body().release();
        }
    }

    private void processMergedBlockMetaRequest(final MergedBlockMetaRequest request){
        try {
            rpcHandler.getMergedBlockMetaReqHandler().receiveMergedBlockMetaReq(reverseClient, request, new MergedBlockMetaResponseCallback() {
                @Override
                public void onSuccess(int numChunks, ManagedBuffer buffer) {
                    logger.trace("Sending meta for request {} numChunks {}",request,numChunks);
                    respond(new MergedBlockMetaSuccess(request.requestId,numChunks,buffer));
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.trace("Failed to send meta for {}",request);
                    respond(new RpcFailure(request.requestId, Throwables.getStackTraceAsString(e)));
                }
            });
        }catch (Exception e){
            logger.error("Error while invoking receiveMergeBlockMetaReq() for appId {} shuffleId {} reduceId {}",
                    request.appId,request.shuffleId,request.appId,e);
            respond(new RpcFailure(request.requestId,Throwables.getStackTraceAsString(e)));
        }
    }


    private ChannelFuture respond(Encodable result){
        SocketAddress remoteAddress = channel.remoteAddress();
        return channel.writeAndFlush(result).addListener(future -> {
            if (future.isSuccess()) {
                logger.trace("Sent result {} to client {}", result,remoteAddress);
            }else {
                logger.error(String.format("Error sending result %s to %s; closing connection",
                        result,remoteAddress),future.cause());
                channel.close();
            }
        });
    }
}
