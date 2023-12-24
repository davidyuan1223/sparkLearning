package org.apache.spark.network.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class OneForOneStreamManager extends StreamManager{
    private static final Logger logger = LoggerFactory.getLogger(OneForOneStreamManager.class);
    private final AtomicLong nextStreamId;
    private final ConcurrentHashMap<Long,StreamState> streams;

    private static class StreamState{
        final String appId;
        final Iterator<ManagedBuffer> buffers;
        final Channel associatedChannel;
        final boolean isBufferMaterializedOnNext;
        int curChunk=0;
        final AtomicLong chunksBeingTransferred = new AtomicLong(0L);

        public StreamState(String appId, Iterator<ManagedBuffer> buffers, Channel associatedChannel, boolean isBufferMaterializedOnNext) {
            this.appId = appId;
            this.buffers = Preconditions.checkNotNull(buffers);
            this.associatedChannel = associatedChannel;
            this.isBufferMaterializedOnNext = isBufferMaterializedOnNext;
        }
    }
    public OneForOneStreamManager(){
        nextStreamId=new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE)*1000);
        streams=new ConcurrentHashMap<>();
    }

    @Override
    public ManagedBuffer getChunk(long streamId, int chunkIndex) {
        StreamState state = streams.get(streamId);
        if (state == null) {
            throw new IllegalStateException(String.format("Requested chunk not available since streamId %s is closed",streamId));
        } else if (chunkIndex != state.curChunk) {
            throw new IllegalStateException(String.format("Received out-of-order chunk index %s (expected %s)",chunkIndex,state.curChunk));
        } else if (!state.buffers.hasNext()) {
            throw new IllegalStateException(String.format("Requested chunk index beyond end %s",chunkIndex));
        }
        state.curChunk+=1;
        ManagedBuffer nextChunk = state.buffers.next();
        if (!state.buffers.hasNext()) {
            logger.trace("Removing stream id {}",streamId);
            streams.remove(streamId);
        }
        return nextChunk;
    }

    @Override
    public ManagedBuffer openStream(String streamChunkId) {
        Pair<Long,Integer> streamChunkIdPair=parseStreamChunkId(streamChunkId);
        return getChunk(streamChunkIdPair.getLeft(),streamChunkIdPair.getRight());
    }

    public static String getStreamChunkId(long streamId, int chunkId){
        return String.format("%d_%d",streamId,chunkId);
    }

    public static Pair<Long,Integer> parseStreamChunkId(String streamChunkId){
        String[] array = streamChunkId.split("_");
        assert array.length==2: "Stream id and chunk index should be specified.";
        Long streamId = Long.valueOf(array[0]);
        int chunkIndex = Integer.valueOf(array[1]);
        return ImmutablePair.of(streamId,chunkIndex);
    }

    @Override
    public void connectionTerminated(Channel channel) {
        RuntimeException failedToReleaseBufferException = null;
        for (Map.Entry<Long, StreamState> entry : streams.entrySet()) {
            StreamState state = entry.getValue();
            if (state.associatedChannel == channel) {
                streams.remove(entry.getKey());
                try {
                    while (!state.isBufferMaterializedOnNext && state.buffers.hasNext()) {
                        ManagedBuffer buffer = state.buffers.next();
                        if (buffer != null) {
                            buffer.release();
                        }
                    }
                }catch (RuntimeException e){
                    if (failedToReleaseBufferException == null) {
                        failedToReleaseBufferException=e;
                    }else {
                        logger.error("Exception trying to release remaining StreamState buffers",e);
                    }
                }
            }
        }
        if (failedToReleaseBufferException != null) {
            throw failedToReleaseBufferException;
        }
    }

    @Override
    public void checkAuthorization(TransportClient client, long streamId) {
        if (client.getClientId() != null) {
            StreamState state = streams.get(streamId);
            Preconditions.checkArgument(state!=null, "Unknown stream ID.");
            if (!client.getClientId().equals(state.appId)) {
                throw new SecurityException(String.format(
                        "Client %s not authorized to read stream %d (app %s).",
                        client.getClientId(),
                        streamId,
                        state.appId
                ));
            }
        }
    }

    @Override
    public void chunkBeingSent(long streamId) {
        StreamState streamState = streams.get(streamId);
        if (streamState != null) {
            streamState.chunksBeingTransferred.incrementAndGet();
        }

    }

    @Override
    public void streamBeingSent(String streamId) {
        chunkBeingSent(parseStreamChunkId(streamId).getLeft());
    }

    @Override
    public void chunkSent(long streamId) {
        StreamState streamState = streams.get(streamId);
        if (streamState != null) {
            streamState.chunksBeingTransferred.decrementAndGet();
        }
    }

    @Override
    public void streamSent(String streamId) {
        chunkSent(OneForOneStreamManager.parseStreamChunkId(streamId).getLeft());
    }

    @Override
    public long chunksBeingTransferred() {
        long sum = 0L;
        for (StreamState streamState: streams.values()) {
            sum += streamState.chunksBeingTransferred.get();
        }
        return sum;
    }

    /**
     * Registers a stream of ManagedBuffers which are served as individual chunks one at a time to
     * callers. Each ManagedBuffer will be release()'d after it is transferred on the wire. If a
     * client connection is closed before the iterator is fully drained, then the remaining
     * materialized buffers will all be release()'d, but some buffers like
     * ShuffleManagedBufferIterator, ShuffleChunkManagedBufferIterator, ManagedBufferIterator should
     * not release, because they have not been materialized before requesting the iterator by
     * the next method.
     *
     * If an app ID is provided, only callers who've authenticated with the given app ID will be
     * allowed to fetch from this stream.
     *
     * This method also associates the stream with a single client connection, which is guaranteed
     * to be the only reader of the stream. Once the connection is closed, the stream will never
     * be used again, enabling cleanup by `connectionTerminated`.
     */
    public long registerStream(
            String appId,
            Iterator<ManagedBuffer> buffers,
            Channel channel,
            boolean isBufferMaterializedOnNext) {
        long myStreamId = nextStreamId.getAndIncrement();
        streams.put(myStreamId, new StreamState(appId, buffers, channel, isBufferMaterializedOnNext));
        return myStreamId;
    }

    public long registerStream(String appId, Iterator<ManagedBuffer> buffers, Channel channel) {
        return registerStream(appId, buffers, channel, false);
    }

    @VisibleForTesting
    public int numStreamStates() {
        return streams.size();
    }
}
