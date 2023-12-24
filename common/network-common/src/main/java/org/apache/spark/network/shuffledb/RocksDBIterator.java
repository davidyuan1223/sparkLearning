package org.apache.spark.network.shuffledb;

import com.google.common.base.Throwables;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class RocksDBIterator implements DBIterator{
    private final RocksIterator it;
    private boolean checkedNext;
    private boolean closed;
    private Map.Entry<byte[], byte[]> next;
    public RocksDBIterator(RocksIterator it){
        this.it=it;
    }

    @Override
    public boolean hasNext() {
        if (!checkedNext && !closed) {
            next=loadNext();
            checkedNext=true;
        }
        if (!closed && next == null) {
            try {
                close();
            }catch (IOException e){
                throw Throwables.propagate(e);
            }
        }
        return next!=null;
    }

    private Map.Entry<byte[], byte[]> loadNext() {
        if (it.isValid()) {
            Map.Entry<byte[], byte[]> nextEntry = new AbstractMap.SimpleEntry<>(it.key(), it.value());
            it.next();
            return nextEntry;
        }
        return null;
    }

    @Override
    public Map.Entry<byte[], byte[]> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        checkedNext=false;
        Map.Entry<byte[], byte[]> ret=next;
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


}
