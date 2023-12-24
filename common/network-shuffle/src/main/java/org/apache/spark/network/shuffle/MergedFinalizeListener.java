package org.apache.spark.network.shuffle;

import org.apache.spark.network.shuffle.protocol.MergeStatuses;

import java.util.EventListener;

public interface MergedFinalizeListener extends EventListener {
    void onShuffleMergeSuccess(MergeStatuses statuses);
    void onShuffleMergeFailure(Throwable e);
}
