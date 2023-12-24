package org.apache.spark.network.client;

public class ChunkFetchFailureException extends RuntimeException{
    public ChunkFetchFailureException(String errMsg, Throwable cause){
        super(errMsg,cause);
    }
    public ChunkFetchFailureException(String errMsg){
        super(errMsg);
    }
}
