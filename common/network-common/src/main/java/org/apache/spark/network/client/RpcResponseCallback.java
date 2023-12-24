package org.apache.spark.network.client;

import java.nio.ByteBuffer;

public interface RpcResponseCallback extends BaseResponseCallback{
    void onSuccess(ByteBuffer response);
}
