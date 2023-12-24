package org.apache.spark.network.shuffle;

import com.google.common.base.Preconditions;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.BlockPushNonFatalFailure;
import org.apache.spark.network.shuffle.protocol.BlockPushReturnCode;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class OneForOneBlockPusher {
    private static final Logger logger = LoggerFactory.getLogger(OneForOneBlockPusher.class);
    private static final ErrorHandler PUSH_ERROR_HANDLER = new ErrorHandler.BlockPushErrorHandler();
    public static final String SHUFFLE_PUSH_BLOCK_PREFIX="shufflePush";
    private final TransportClient client;
    private final String appId;
    private final int appAttemptId;
    private final String[] blockIds;
    private final BlockPushingListener listener;
    private final Map<String ,ManagedBuffer> buffers;
    public OneForOneBlockPusher(TransportClient client, String appId, int comparableAppAttemptId, String[] blockIds, BlockPushingListener listener, Map<String, ManagedBuffer> buffersWithId) {
        this.client=client;
        this.appId=appId;
        this.appAttemptId=comparableAppAttemptId;
        this.blockIds=blockIds;
        this.listener=listener;
        this.buffers=buffersWithId;
    }

    private class BlockPushCallback implements RpcResponseCallback{
        private int index;
        private String blockId;
        BlockPushCallback(int index,String blockId){
            this.index=index;
            this.blockId=blockId;
        }

        @Override
        public void onSuccess(ByteBuffer response) {
            BlockPushReturnCode pushResponse=(BlockPushReturnCode) BlockTransferMessage.Decoder.fromByteBuffer(response);
            BlockPushNonFatalFailure.ReturnCode returnCode = BlockPushNonFatalFailure.getReturnCode(pushResponse.returnCode);
            if (returnCode != BlockPushNonFatalFailure.ReturnCode.SUCCESS) {
                String blockId = pushResponse.failureBlockId;
                Preconditions.checkArgument(!blockId.isEmpty());
                checkAndFailRemainingBlocks(index,new BlockPushNonFatalFailure(returnCode,BlockPushNonFatalFailure.getErrorMsg(blockId,returnCode)));
            }else {
                listener.onBlockPushSuccess(blockId,new NioManagedBuffer(ByteBuffer.allocate(0)));
            }
        }

        @Override
        public void onFailure(Throwable e) {
            checkAndFailRemainingBlocks(index,e);
        }
    }

    private void checkAndFailRemainingBlocks(int index,Throwable e){
        if (PUSH_ERROR_HANDLER.shouldRetryError(e)) {
            String[] targetBlockId = Arrays.copyOfRange(blockIds, index, index + 1);
            failRemainingBlocks(targetBlockId,e);
        }else {
            String[] targetBlockId = Arrays.copyOfRange(blockIds, index, blockIds.length);
            failRemainingBlocks(targetBlockId, e);
        }
    }

    private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
        for (String blockId : failedBlockIds) {
            try {
                listener.onBlockPushFailure(blockId, e);
            } catch (Exception e2) {
                logger.error("Error in block push failure callback", e2);
            }
        }
    }

    public void start(){
        logger.debug("Start pushing {} blocks",blockIds.length);
        for (int i = 0; i < blockIds.length; i++) {
            assert buffers.containsKey(blockIds[i]) : "Could not find the block buffer for block "+blockIds[i];
            String[] blockIdParts = blockIds[i].split("_");
            if (blockIdParts.length != 5 || !blockIdParts[0].equals(SHUFFLE_PUSH_BLOCK_PREFIX)) {
                throw new IllegalArgumentException("Unexpected shuffle push block id format: "+blockIds[i]);
            }
            ByteBuffer header =
                    new PushBlockStream(appId,appAttemptId,Integer.parseInt(blockIdParts[1]),
                            Integer.parseInt(blockIdParts[2]),Integer.parseInt(blockIdParts[3]),
                            Integer.parseInt(blockIdParts[4]),i).toByteBuffer();
            client.uploadStream(new NioManagedBuffer(header),buffers.get(blockIds[i]),new BlockPushCallback(i,blockIds[i]));
        }
    }
}
