package org.apache.spark.network.shuffle;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.shuffle.MergedBlockMeta;
import org.apache.spark.network.shuffle.MergedShuffleFileManager;
import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.util.TransportConf;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class NoOpMergedShuffleFileManager implements MergedShuffleFileManager {
    public NoOpMergedShuffleFileManager(TransportConf conf, File recoveryFile) {

    }

    @Override
    public StreamCallbackWithID receiveBlockDataAsStream(PushBlockStream msg) {
        throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public MergeStatuses finalizeShuffleMerge(FinalizeShuffleMerge msg) throws IOException {
        throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public void registerExecutor(String appId, ExecutorShuffleInfo executorInfo) {

    }

    @Override
    public void applicationRemoved(String appId, boolean cleanupLocalDirs) {

    }

    @Override
    public ManagedBuffer getMergedBlockData(String appId, int shuffleId, int shuffleMergeId, int reduceId, int chunkId) {
        throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public MergedBlockMeta getMergedBlockMeta(String appId, int shuffleId, int shuffleMergeId, int reduceId) {
        throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public String[] getMergedBlockDirs(String appId) {
        throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public void removeShuffleMerge(RemoveShuffleMerge removeShuffleMerge) {
        throw new UnsupportedOperationException("Cannot handle merged shuffle remove");
    }
}
