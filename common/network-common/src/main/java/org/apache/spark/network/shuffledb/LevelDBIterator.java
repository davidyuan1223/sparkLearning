package org.apache.spark.network.shuffledb;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

public class LevelDBIterator implements DBIterator {
    private final org.iq80.leveldb.DBIterator it;
    private boolean checkNext;
    private boolean closed;
    private Map.Entry<byte[],byte[]> next;
    public LevelDBIterator(org.iq80.leveldb.DBIterator iterator) {
        this.it=iterator;
    }

    @Override
    public boolean hasNext() {
        if (!checkNext && !closed) {
            next=loadNext();
            checkNext=true;
        }
        if (!closed && next == null) {
            try {
                close();
            }catch (IOException ioe){
                throw Throwables.propagate(ioe);
            }
        }
        return next!=null;
    }

    @Override
    public Map.Entry<byte[], byte[]> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        checkNext=false;
        Map.Entry<byte[],byte[]> ret = next;
        next=null;
        return ret;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            it.close();
            closed=true;
            next=null;
        }
    }

    @Override
    public void seek(byte[] key) {
        it.seek(key);
    }

    private Map.Entry<byte[],byte[]> loadNext(){
        boolean hasNext = it.hasNext();
        if (!hasNext) {
            return null;
        }
        return it.next();
    }
}
