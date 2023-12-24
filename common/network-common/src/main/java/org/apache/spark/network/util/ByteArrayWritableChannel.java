package org.apache.spark.network.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ByteArrayWritableChannel implements WritableByteChannel {
    private final byte[] data;
    private int offset;
    public ByteArrayWritableChannel(int size){
        data=new byte[size];
    }

    public byte[] getData() {
        return data;
    }
    public int length(){
        return offset;
    }
    public void reset(){
        offset=0;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int toTransfer = Math.min(src.remaining(), data.length - offset);
        src.get(data,offset,toTransfer);
        offset+=toTransfer;
        return toTransfer;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean isOpen() {
        return true;
    }
}
