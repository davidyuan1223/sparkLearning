package org.apache.spark.network.client;

import org.apache.spark.network.buffer.ManagedBuffer;

public interface MergedBlockMetaResponseCallback extends BaseResponseCallback{
    void onSuccess(int numChunks, ManagedBuffer buffer);
}
