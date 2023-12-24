package org.apache.spark.network.shuffle;

import org.apache.spark.network.buffer.ManagedBuffer;

public interface BlockFetchingListener extends BlockTransferListener{
    void onBlockFetchSuccess(String blockId, ManagedBuffer data);
    void onBlockFetchFailure(String blockId,Throwable exception);

    @Override
    default void onBlockTransferSuccess(String blockId, ManagedBuffer data){
        onBlockFetchSuccess(blockId,data);
    }

    @Override
    default void onBlockTransferFailure(String blockId, Throwable exception){
        onBlockFetchFailure(blockId, exception);
    }

    @Override
    default String getTransferType(){
        return "fetch";
    }
}

