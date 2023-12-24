package org.apache.spark.network.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyLogger {
    private static final Logger logger = LoggerFactory.getLogger(NettyLogger.class);
    private static class NoContentLoggingHandler extends LoggingHandler {
        public NoContentLoggingHandler(Class<?> clazz, LogLevel level) {
            super(clazz, level);
        }

        @Override
        protected String format(ChannelHandlerContext ctx, String eventName, Object arg) {
            if (arg instanceof ByteBuf) {
                return format(ctx,eventName)+" "+((ByteBuf)arg).readableBytes()+"B";
            } else if (arg instanceof ByteBufHolder) {
                return format(ctx,eventName)+" "+
                        ((ByteBufHolder)arg).content().readableBytes()+"B";
            }else {
                return super.format(ctx,eventName,arg);
            }
        }
    }
    private final LoggingHandler loggingHandler;

    public NettyLogger(){
        if (logger.isTraceEnabled()) {
            loggingHandler=new LoggingHandler(NettyLogger.class,LogLevel.TRACE);
        } else if (logger.isDebugEnabled()) {
            loggingHandler=new LoggingHandler(NettyLogger.class,LogLevel.DEBUG);
        }else {
            loggingHandler=null;
        }
    }

    public LoggingHandler getLoggingHandler() {
        return loggingHandler;
    }
}
