package org.apache.spark.network.client;


import io.netty.channel.Channel;

public interface TransportClientBootstrap {
    void doBootstrap(TransportClient client, Channel channel)throws RuntimeException;
}
