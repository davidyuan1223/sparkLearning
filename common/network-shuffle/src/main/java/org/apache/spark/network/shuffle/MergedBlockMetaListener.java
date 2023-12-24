package org.apache.spark.network.shuffle;

import java.util.EventListener;

public interface MergedBlockMetaListener extends EventListener {
    void onSuccess(int shuffleId,int shuffleMergeId,int reduceId,MergedBlockMeta meta);
    void onFailure(int shuffleId,int shuffleMergeId,int reduceId,Throwable exception);
}
