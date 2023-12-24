package org.apache.spark.network.server;

import io.netty.channel.Channel;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

public abstract class StreamManager {
    public abstract ManagedBuffer getChunk(long streamId, int chunkIndex);
    public ManagedBuffer openStream(String streamId){
        throw new UnsupportedOperationException();
    }
    public void connectionTerminated(Channel channel){}
    public void checkAuthorization(TransportClient client, long streamId){}
    public long chunksBeingTransferred(){
        return 0;
    }
    public void chunkBeingSent(long streamId){}
    public void streamBeingSent(String streamId){}
    public void chunkSent(long streamId){}
    public void streamSent(String streamId){}
}
