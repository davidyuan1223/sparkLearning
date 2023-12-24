package org.apache.spark.network;

import com.codahale.metrics.Counter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.server.*;
import org.apache.spark.network.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportContext implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);
    private static final NettyLogger nettyLogger = new NettyLogger();
    private final TransportConf conf;
    private final RpcHandler rpcHandler;
    private final boolean closeIdleConnections;
    private Counter registeredConnections = new Counter();
    private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
    private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;
    private final EventLoopGroup chunkFetchWorkers;
    public TransportContext(TransportConf conf, RpcHandler rpcHandler){
        this(conf,rpcHandler,false,false);
    }
    public TransportContext(TransportConf conf,RpcHandler rpcHandler,boolean closeIdleConnections){
        this(conf,rpcHandler,closeIdleConnections,false);
    }
    public TransportContext(TransportConf conf,RpcHandler rpcHandler,boolean closeIdleConnections,boolean isClientOnly){
        this.conf=conf;
        this.rpcHandler=rpcHandler;
        this.closeIdleConnections=closeIdleConnections;
        if (conf.getModuleName() != null &&
                conf.getModuleName().equalsIgnoreCase("shuffle") &&
                !isClientOnly &&
                conf.separateChunkFetchRequest()) {
            chunkFetchWorkers= NettyUtils.createEventLoop(
                    IOMode.valueOf(conf.ioMode()),
                    conf.chunkFetchHandlerThreads(),
                    "shuffle-chunk-fetch-handler"
            );
        }else {
            chunkFetchWorkers=null;
        }
    }
    public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps){
        return new TransportClientFactory(this,bootstraps);
    }
    public TransportClientFactory createClientFactory(){
        return createClientFactory(new ArrayList<>());
    }
    public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps){
        return new TransportServer(this,null,port,rpcHandler,bootstraps);
    }
    public TransportServer createServer(
            String host, int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, host, port, rpcHandler, bootstraps);
    }
    public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
        return createServer(0, bootstraps);
    }

    public TransportServer createServer() {
        return createServer(0, new ArrayList<>());
    }
    public TransportChannelHandler initializePipeline(SocketChannel channel){
        return initializePipeline(channel,rpcHandler);
    }
    public TransportChannelHandler initializePipeline(SocketChannel channel,
                                                      RpcHandler rpcHandler){
        try {
            TransportChannelHandler channelHandler=createChannelHandler(channel,rpcHandler);
            ChannelPipeline pipeline = channel.pipeline();
            if (nettyLogger.getLoggingHandler() != null) {
                pipeline.addLast("loggingHandler",nettyLogger.getLoggingHandler());
            }
            pipeline
                    .addLast("encoder",ENCODER)
                    .addLast(TransportFrameDecoder.HANDLER_NAME,NettyUtils.createFrameDecoder())
                    .addLast("decoder",DECODER)
                    .addLast("idleStateHandler",
                            new IdleStateHandler(0,0,conf.connectionTimeoutMs()))
                    .addLast("handler",channelHandler);
            if (chunkFetchWorkers != null) {
                ChunkFetchRequestHandler chunkFetchRequestHandler = new ChunkFetchRequestHandler(channelHandler.getClient(), this.rpcHandler.getStreamManager(),
                        conf.maxChunksBeingTransferred(), true);
                pipeline.addLast(chunkFetchWorkers,"chunkFetchHandler",chunkFetchRequestHandler);
            }
            return channelHandler;
        }catch (RuntimeException e){
            logger.error("Error while initializing Netty pipeline",e);
            throw e;
        }
    }

    private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
        TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
        TransportClient client = new TransportClient(channel, responseHandler);
        boolean separateChunkFetchRequest = conf.separateChunkFetchRequest();
        ChunkFetchRequestHandler chunkFetchRequestHandler=null;
        if (!separateChunkFetchRequest) {
            chunkFetchRequestHandler=new ChunkFetchRequestHandler(
                    client,rpcHandler.getStreamManager(),conf.maxChunksBeingTransferred(),false
            );
        }
        TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client, rpcHandler, conf.maxIORetries(), chunkFetchRequestHandler);
        return new TransportChannelHandler(client,requestHandler,responseHandler,
                conf.connectionTimeoutMs(),separateChunkFetchRequest,closeIdleConnections,this);
    }

    public TransportConf getConf() {
        return conf;
    }

    public Counter getRegisteredConnections() {
        return registeredConnections;
    }

    @Override
    public void close() throws IOException {
        if (chunkFetchWorkers != null) {
            chunkFetchWorkers.shutdownGracefully();
        }
    }
}
