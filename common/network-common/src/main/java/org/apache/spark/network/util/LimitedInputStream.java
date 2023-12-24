package org.apache.spark.network.util;

import com.google.common.base.Preconditions;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public final class LimitedInputStream extends FilterInputStream {
    private final boolean closeWrappedStream;
    private long left;
    private long mark=-1;
    public LimitedInputStream(InputStream in, long limit){
        this(in,limit,true);
    }
    public LimitedInputStream(InputStream in,long limit,boolean closeWrappedStream){
        super(in);
        this.closeWrappedStream=closeWrappedStream;
        Preconditions.checkNotNull(in);
        Preconditions.checkArgument(limit>=0,"limit must be non-negative");
        left=limit;
    }

    @Override
    public int available() throws IOException {
        return (int)Math.min(in.available(),left);
    }

    @Override
    public synchronized void mark(int readlimit) {
        in.mark(readlimit);
        mark=left;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (left==0) {
            return -1;
        }
        int result = in.read();
        if (result!=-1) {
            --left;
        }
        return result;
    }

    @Override
    public synchronized void reset() throws IOException {
        if (!in.markSupported()) {
            throw new IOException("Mark not supported");
        }
        if (mark==-1) {
            throw new IOException("Mark not set");
        }
        in.reset();
        left=mark;
    }

    @Override
    public long skip(long n) throws IOException {
        n=Math.min(n,left);
        long skipped = in.skip(n);
        left-=skipped;
        return skipped;
    }

    @Override
    public void close() throws IOException {
        if (closeWrappedStream) {
            super.close();
        }
    }
}
