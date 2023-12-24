package org.apache.spark.network.server;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.client.TransportClient;

import java.nio.ByteBuffer;

public abstract class AbstractAuthRpcHandler extends RpcHandler{
    private final RpcHandler delegate;
    private boolean isAuthenticated;
    protected AbstractAuthRpcHandler(RpcHandler delegate){
        this.delegate=delegate;
    }
    protected abstract boolean doAuthChallenge(TransportClient client, ByteBuffer message, RpcResponseCallback callback);

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        if (isAuthenticated) {
            delegate.receive(client,message,callback);
        }else {
            isAuthenticated=doAuthChallenge(client,message,callback);
        }
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message) {
        if (isAuthenticated) {
            delegate.receive(client,message);
        }else {
            throw new SecurityException("Unauthenticated call to receive().");
        }
    }

    @Override
    public StreamCallbackWithID receiveStream(TransportClient client, ByteBuffer messageHeader, RpcResponseCallback callback) {
        if (isAuthenticated) {
            return delegate.receiveStream(client,messageHeader,callback);
        }else {
            throw new SecurityException("Unauthenticated call to receiveStream().");
        }
    }

    @Override
    public StreamManager getStreamManager() {
        return delegate.getStreamManager();
    }

    @Override
    public void channelActive(TransportClient client) {
        delegate.channelActive(client);
    }

    @Override
    public void channelInActive(TransportClient client) {
        delegate.channelInActive(client);
    }

    @Override
    public void exceptionCaught(Throwable cause, TransportClient client) {
        delegate.exceptionCaught(cause,client);
    }

    public boolean isAuthenticated() {
        return isAuthenticated;
    }
    public MergedBlockMetaReqHandler getMergedBlockMetaReqHandler() {
        return delegate.getMergedBlockMetaReqHandler();
    }
}
