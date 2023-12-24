package org.apache.spark.network.protocol;

import org.apache.spark.network.buffer.ManagedBuffer;

public abstract class AbstractResponseMessage extends AbstractMessage implements ResponseMessage{
    protected AbstractResponseMessage(ManagedBuffer body, boolean isBodyInFrame){
        super(body,isBodyInFrame);
    }
    public abstract ResponseMessage createFailureResponse(String error);
}
