package org.apache.spark.network.shuffle;

import org.apache.spark.network.buffer.ManagedBuffer;

public interface BlockPushingListener extends BlockTransferListener{
    void onBlockPushSuccess(String blockId, ManagedBuffer data);
    void onBlockPushFailure(String blockId, Throwable exception);

    @Override
    default void onBlockTransferSuccess(String blockId, ManagedBuffer data){
        onBlockPushSuccess(blockId, data);
    }

    @Override
    default void onBlockTransferFailure(String blockId, Throwable exception){
        onBlockPushFailure(blockId, exception);
    }

    @Override
    default String getTransferType(){
        return "push";
    }
}
