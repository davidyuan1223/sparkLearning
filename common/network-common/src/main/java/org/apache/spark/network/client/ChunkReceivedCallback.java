package org.apache.spark.network.client;

import org.apache.spark.network.buffer.ManagedBuffer;

public interface ChunkReceivedCallback {
    void onSuccess(int chunkIndex, ManagedBuffer buffer);
    void onFailure(int chunkIndex, Throwable e);
}
