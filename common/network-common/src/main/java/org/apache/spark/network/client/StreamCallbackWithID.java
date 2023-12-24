package org.apache.spark.network.client;

import java.nio.ByteBuffer;

public interface StreamCallbackWithID extends StreamCallback{
    String getID();
    default ByteBuffer getCompletionResponse(){
        return ByteBuffer.allocate(0);
    }
}
