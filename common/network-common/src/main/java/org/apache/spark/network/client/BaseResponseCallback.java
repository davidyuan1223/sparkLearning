package org.apache.spark.network.client;

public interface BaseResponseCallback {
    void onFailure(Throwable e);
}
