package org.apache.spark.network.server;

import org.apache.spark.network.client.MergedBlockMetaResponseCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.MergedBlockMetaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public abstract class RpcHandler {
    private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();
    private static final MergedBlockMetaReqHandler NOOP_MERGED_BLOCK_META_REQ_HANDLER=new NoopMergedBlockMetaReqHandler();
    public abstract void receive(TransportClient client,ByteBuffer message,RpcResponseCallback callback);

    public StreamCallbackWithID receiveStream(TransportClient client,ByteBuffer messageHeader, RpcResponseCallback callback){
        throw new UnsupportedOperationException();
    }
    public abstract StreamManager getStreamManager();
    public void receive(TransportClient client, ByteBuffer message){
        receive(client,message,ONE_WAY_CALLBACK);
    }
    public MergedBlockMetaReqHandler getMergedBlockMetaReqHandler(){
        return NOOP_MERGED_BLOCK_META_REQ_HANDLER;
    }
    public void channelActive(TransportClient client){}
    public void channelInActive(TransportClient client){}
    public void exceptionCaught(Throwable cause, TransportClient client){}
    private static class OneWayRpcCallback implements RpcResponseCallback{
        private static final Logger logger = LoggerFactory.getLogger(OneWayRpcCallback.class);

        @Override
        public void onSuccess(ByteBuffer response) {
            logger.warn("Response provided for one-way RPC.");
        }

        @Override
        public void onFailure(Throwable e) {
            logger.error("Error response provide for one-way RPC.", e);
        }
    }
    public interface MergedBlockMetaReqHandler {
        void receiveMergedBlockMetaReq(TransportClient client, MergedBlockMetaRequest mergedBlockMetaRequest, MergedBlockMetaResponseCallback callback);
    }
    private static class NoopMergedBlockMetaReqHandler implements MergedBlockMetaReqHandler{
        @Override
        public void receiveMergedBlockMetaReq(TransportClient client, MergedBlockMetaRequest mergedBlockMetaRequest, MergedBlockMetaResponseCallback callback) {

        }
    }

}
