package org.apache.spark.network.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.network.shuffledb.DB;
import org.apache.spark.network.shuffledb.DBBackend;
import org.apache.spark.network.shuffledb.LevelDB;
import org.apache.spark.network.shuffledb.StoreVersion;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;

public class DBProvider {
    public static DB initDB(DBBackend dbBackend,
                            File dbFile,
                            StoreVersion version,
                            ObjectMapper mapper)throws IOException{
        if (dbFile != null) {
            switch (dbBackend) {
                case LEVELDB:
                    org.iq80.leveldb.DB levelDB = LevelDBProvider.initLevelDB(dbFile, version, mapper);
                    return levelDB!=null ? new LevelDB(levelDB): null;
                case ROCKSDB:
                    RocksDB rocksDB = RocksDBProvider.initRockDB(dbFile, version, mapper);
                    return rocksDB!=null?new org.apache.spark.network.shuffledb.RocksDB(rocksDB):null;
                default:
                    throw new IllegalArgumentException("Unsupported DBBackend: "+dbBackend);
            }
        }
        return null;
    }

    @VisibleForTesting
    public static DB initDB(DBBackend dbBackend, File file) throws IOException {
        if (file != null) {
            switch (dbBackend) {
                case LEVELDB: return new LevelDB(LevelDBProvider.initLevelDB(file));
                case ROCKSDB: return new org.apache.spark.network.shuffledb.RocksDB(RocksDBProvider.initRocksDB(file));
                default:
                    throw new IllegalArgumentException("Unsupported DBBackend: " + dbBackend);
            }
        }
        return null;
    }
}
