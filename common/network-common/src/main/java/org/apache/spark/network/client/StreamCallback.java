package org.apache.spark.network.client;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface StreamCallback {
    void onData(String streamId, ByteBuffer buf)throws IOException;
    void onComplete(String streamId)throws IOException;
    void onFailure(String streamId,Throwable cause)throws IOException;
}
