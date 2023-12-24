package org.apache.spark.network.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.RequestMessage;
import org.apache.spark.network.protocol.ResponseMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.spark.network.util.NettyUtils.*;

public class TransportChannelHandler extends SimpleChannelInboundHandler<Message> {
    private static final Logger logger = LoggerFactory.getLogger(TransportChannelHandler.class);
    private final TransportClient client;
    private final TransportResponseHandler responseHandler;
    private final TransportRequestHandler requestHandler;
    private final long requestTimeoutNs;
    private final boolean closeIdleConnections;
    private final boolean skipChunkFetchRequest;
    private final TransportContext transportContext;
    public TransportChannelHandler(
            TransportClient client,
            TransportRequestHandler requestHandler,
            TransportResponseHandler responseHandler,
            long requestTimeoutNs,
            boolean skipChunkFetchRequest,
            boolean closeIdleConnections,
            TransportContext transportContext
    ){
        this.client=client;
        this.responseHandler=responseHandler;
        this.requestHandler=requestHandler;
        this.requestTimeoutNs=requestTimeoutNs*1000L*1000;
        this.skipChunkFetchRequest=skipChunkFetchRequest;
        this.closeIdleConnections=closeIdleConnections;
        this.transportContext=transportContext;
    }

    public TransportClient getClient() {
        return client;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn("Exception in connection from "+getRemoteAddress(ctx.channel()),cause);
        requestHandler.exceptionCaught(cause);
        responseHandler.exceptionCaught(cause);
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        try{
            requestHandler.channelActive();
        }catch (RuntimeException e){
            logger.error("Exception from request handler while channel is active",e);
        }
        try {
            responseHandler.channelActive();
        }catch (RuntimeException e){
            logger.error("Exception from response handler while channel is active",e);
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            requestHandler.channelInactive();
        }catch (RuntimeException e){
            logger.error("Exception from request handler while channel is inactive",e);
        }
        try {
            responseHandler.channelInactive();
        }catch (RuntimeException e){
            logger.error("Exception from response handler while channel is inactive",e);
        }
        super.channelInactive(ctx);
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        if (skipChunkFetchRequest && msg instanceof ChunkFetchRequest) {
            return false;
        }else {
            return super.acceptInboundMessage(msg);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Message message) throws Exception {
        if (message instanceof RequestMessage) {
            requestHandler.handle((RequestMessage) message);
        } else if (message instanceof ResponseMessage) {
            responseHandler.handle((ResponseMessage) message);
        }else {
            channelHandlerContext.fireChannelRead(message);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            synchronized (this) {
                boolean isActuallyOverdue =
                        System.nanoTime()-responseHandler.getTimeOfLastRequestNs()>requestTimeoutNs;
                if (e.state() == IdleState.ALL_IDLE && isActuallyOverdue) {
                    if (responseHandler.hasOutstandingRequests()) {
                        String address = getRemoteAddress(ctx.channel());
                        logger.warn("Connection to {} has been quiet for {} ms while there are outstanding " +
                                "requests. Assuming connection is dead; please adjust " +
                                "spark.{}.io.connectionTimeout if this is wrong.",
                                address,requestTimeoutNs/1000/1000,transportContext.getConf().getModuleName());
                        client.timeOut();
                        ctx.close();
                    } else if (closeIdleConnections) {
                        client.timeOut();
                        ctx.close();
                    }
                }
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    public TransportResponseHandler getResponseHandler(){
        return responseHandler;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        transportContext.getRegisteredConnections().inc();
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        transportContext.getRegisteredConnections().dec(0);
        super.channelUnregistered(ctx);
    }
}


