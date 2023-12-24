package org.apache.spark.network.shuffledb;

import java.io.IOException;

public class LevelDB implements DB{
    private final org.iq80.leveldb.DB db;

    public LevelDB(org.iq80.leveldb.DB db) {
        this.db = db;
    }

    @Override
    public void put(byte[] key, byte[] value) {
        db.put(key,value);
    }

    @Override
    public byte[] get(byte[] key) {
        return db.get(key);
    }

    @Override
    public void delete(byte[] key) {
        db.delete(key);
    }

    @Override
    public DBIterator iterator() {
        return new LevelDBIterator(db.iterator());
    }

    @Override
    public void close() throws IOException {
        db.close();
    }
}
