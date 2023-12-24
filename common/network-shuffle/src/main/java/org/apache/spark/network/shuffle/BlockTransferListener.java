package org.apache.spark.network.shuffle;

import org.apache.spark.network.buffer.ManagedBuffer;

import java.util.EventListener;

public interface BlockTransferListener extends EventListener {
    void onBlockTransferSuccess(String blockId, ManagedBuffer data);
    void onBlockTransferFailure(String blockId,Throwable exception);
    String getTransferType();
}
