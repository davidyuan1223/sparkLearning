package org.apache.spark.network.server;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;

import java.nio.ByteBuffer;

public class NoOpRpcHandler extends RpcHandler{
    private final StreamManager streamManager;
    public NoOpRpcHandler(){
        streamManager=new OneForOneStreamManager();
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        throw new UnsupportedOperationException("Cannot handle messages");
    }

    @Override
    public StreamManager getStreamManager() {
        return streamManager;
    }
}
