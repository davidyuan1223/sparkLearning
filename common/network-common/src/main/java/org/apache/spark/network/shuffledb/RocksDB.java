package org.apache.spark.network.shuffledb;

import com.google.common.base.Throwables;
import org.rocksdb.RocksDBException;

import java.io.IOException;

public class RocksDB implements DB{
    private final org.rocksdb.RocksDB db;

    public RocksDB(org.rocksdb.RocksDB db) {
        this.db = db;
    }

    @Override
    public void put(byte[] key, byte[] value) {
        try {
            db.put(key,value);
        }catch (RocksDBException e){
            throw Throwables.propagate(e);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return db.get(key);
        }catch (RocksDBException e){
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void delete(byte[] key) {
        try {
            db.delete(key);
        }catch (RocksDBException e){
            throw Throwables.propagate(e);
        }
    }

    @Override
    public DBIterator iterator() {
        return new RocksDBIterator(db.newIterator());
    }

    @Override
    public void close() throws IOException {
        db.close();
    }
}
