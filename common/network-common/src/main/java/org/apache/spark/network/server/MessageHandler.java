package org.apache.spark.network.server;

import org.apache.spark.network.protocol.Message;

public abstract class MessageHandler <T extends Message> {
    public abstract void handle(T message) throws Exception;
    public abstract void channelActive();
    public abstract void exceptionCaught(Throwable cause);
    public abstract void channelInactive();
}
