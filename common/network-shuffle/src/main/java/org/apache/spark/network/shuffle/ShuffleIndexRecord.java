package org.apache.spark.network.shuffle;

public class ShuffleIndexRecord {
    private final long offset;
    private final long length;
    public ShuffleIndexRecord(long offset, long length){
        this.offset=offset;
        this.length=length;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }
}
