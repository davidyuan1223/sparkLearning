package org.apache.spark.network.server;

import com.google.common.base.Throwables;
import io.netty.channel.*;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.Encodable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.SocketAddress;

import static org.apache.spark.network.util.NettyUtils.*;
public class ChunkFetchRequestHandler extends SimpleChannelInboundHandler<ChunkFetchRequest> {
    private static final Logger logger = LoggerFactory.getLogger(ChunkFetchRequestHandler.class);
    private final TransportClient client;
    private final StreamManager streamManager;
    private final long maxChunksBeingTransferred;
    private final boolean syncModeEnabled;
    public ChunkFetchRequestHandler(TransportClient client,
                                    StreamManager streamManager,
                                    Long maxChunksBeingTransferred,
                                    boolean syncModeEnabled){
        this.client=client;
        this.streamManager=streamManager;
        this.maxChunksBeingTransferred=maxChunksBeingTransferred;
        this.syncModeEnabled=syncModeEnabled;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn("Exception in connection from "+getRemoteAddress(ctx.channel()),cause);
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext,final ChunkFetchRequest chunkFetchRequest) throws Exception {
        Channel channel = channelHandlerContext.channel();
        processFetchRequest(channel,chunkFetchRequest);
    }

    public void processFetchRequest(final Channel channel, final ChunkFetchRequest msg)throws Exception{
        if (logger.isTraceEnabled()) {
            logger.trace("Received req from {} to fetch block {}", getRemoteAddress(channel),
                    msg.streamChunkId);
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
        try {
            streamManager.checkAuthorization(client,msg.streamChunkId.streamId);
            buf=streamManager.getChunk(msg.streamChunkId.streamId,msg.streamChunkId.chunkIndex);
            if (buf == null) {
                throw new IllegalArgumentException("Chunk was not found");
            }
        }catch (Exception e){
            logger.error(String.format("Error opening block %s for request from %s"),
                    msg.streamChunkId,getRemoteAddress(channel),e);
            respond(channel,new ChunkFetchFailure(msg.streamChunkId,
                    Throwables.getStackTraceAsString(e)));
            return;
        }
        streamManager.chunkBeingSent(msg.streamChunkId.streamId);
        respond(channel,new ChunkFetchSuccess(msg.streamChunkId,buf))
                .addListener(
                        (ChannelFutureListener)future -> streamManager.chunkSent(msg.streamChunkId.streamId)
                );
    }

    private ChannelFuture respond(final Channel channel, final Encodable result)throws InterruptedException{
        final SocketAddress remoteAddress = channel.remoteAddress();
        ChannelFuture channelFuture;
        if (syncModeEnabled) {
            channelFuture=channel.writeAndFlush(result).await();
        }else {
            channelFuture=channel.writeAndFlush(result);
        }
        return channelFuture.addListener((ChannelFutureListener)future -> {
            if (future.isSuccess()) {
                logger.trace("Sent result {} to client {}", result, remoteAddress);
            }else {
                logger.error(String.format("Error sending result %s to %s; closing connection",
                        result, remoteAddress),future.cause());
                channel.close();
            }
        });
    }
}
