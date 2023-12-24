package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@ChannelHandler.Sharable
public final class MessageEncoder extends MessageToMessageEncoder<Message> {
    private static final Logger logger= LoggerFactory.getLogger(MessageEncoder.class);
    public static final MessageEncoder INSTANCE = new MessageEncoder();
    private MessageEncoder(){}

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Message message, List<Object> list) throws Exception {
        Object body=null;
        long bodyLength=0;
        boolean isBodyInFrame=false;
        if (message.body() != null) {
            try {
                bodyLength=message.body().size();
                body=message.body().convertToNetty();
                isBodyInFrame=message.isBodyInFrame();
            }catch (Exception e){
                message.body().release();
                if (message instanceof AbstractResponseMessage) {
                    AbstractResponseMessage resp = (AbstractResponseMessage) message;
                    String error = e.getMessage() != null ? e.getMessage() : "null";
                    logger.error(String.format("Error processing %s for client %s",message,
                            channelHandlerContext.channel().remoteAddress()),e);
                }else {
                    throw e;
                }
                return;
            }
        }
        Message.Type msgType = message.type();
        int headerLength = 8 + msgType.encodedLength() + message.encodedLength();
        long frameLength = headerLength + (isBodyInFrame ? bodyLength : 0);
        ByteBuf header = channelHandlerContext.alloc().buffer(headerLength);
        header.writeLong(frameLength);
        msgType.encode(header);
        message.encode(header);
        assert header.writableBytes()==0;
        if (body != null) {
            list.add(new MessageWithHeader(message.body(),header,body,bodyLength));
        }else {
            list.add(header);
        }
    }

}
