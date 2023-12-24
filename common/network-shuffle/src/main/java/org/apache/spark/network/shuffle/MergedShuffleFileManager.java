package org.apache.spark.network.shuffle;

import com.codahale.metrics.MetricSet;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.shuffle.MergedBlockMeta;
import org.apache.spark.network.shuffle.protocol.*;

import java.io.IOException;
import java.util.Collections;

@Evolving
public interface MergedShuffleFileManager {
    StreamCallbackWithID receiveBlockDataAsStream(PushBlockStream msg);
    MergeStatuses finalizeShuffleMerge(FinalizeShuffleMerge msg)throws IOException;
    void registerExecutor(String appId, ExecutorShuffleInfo executorInfo);
    void applicationRemoved(String appId,boolean cleanupLocalDirs);
    ManagedBuffer getMergedBlockData(String appId,int shuffleId,int shuffleMergeId,int reduceId,int chunkId);
    MergedBlockMeta getMergedBlockMeta(String appId,int shuffleId,int shuffleMergeId,int reduceId);
    String[] getMergedBlockDirs(String appId);
    void removeShuffleMerge(RemoveShuffleMerge removeShuffleMerge);
    default void close() {}
    default MetricSet getMetrics(){
        return Collections::emptyMap;
    }
}
