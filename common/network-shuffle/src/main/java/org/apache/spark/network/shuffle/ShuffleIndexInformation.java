package org.apache.spark.network.shuffle;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.file.Files;

public class ShuffleIndexInformation {
    static final int INSTANCE_MEMORY_FOOTPRINT=176;
    private final LongBuffer offsets;
    public ShuffleIndexInformation(String indexFilePath)throws IOException{
        File indexFile = new File(indexFilePath);
        ByteBuffer buffer = ByteBuffer.allocate((int) indexFile.length());
        offsets=buffer.asLongBuffer();
        try(DataInputStream dis = new DataInputStream(Files.newInputStream(indexFile.toPath()))){
            dis.readFully(buffer.array());
        }
    }

    public int getRetainedMemorySize(){
        return (offsets.capacity()<<2) + INSTANCE_MEMORY_FOOTPRINT;
    }

    public ShuffleIndexRecord getIndex(int reduceId){
        return getIndex(reduceId,reduceId+1);
    }

    public ShuffleIndexRecord getIndex(int startReduceId, int endReduceId){
        long offset = offsets.get(startReduceId);
        long nextOffset = offsets.get(endReduceId);
        return new ShuffleIndexRecord(offset,nextOffset-offset);
    }
}
