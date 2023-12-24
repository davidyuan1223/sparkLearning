package org.apache.spark.network.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.network.shuffledb.StoreVersion;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class RocksDBProvider {
    static {
        RocksDB.loadLibrary();
    }
    private static final Logger logger= LoggerFactory.getLogger(RocksDBProvider.class);
    public static RocksDB initRockDB(File dbFile, StoreVersion version, ObjectMapper mapper)throws IOException{
        RocksDB tmpDb=null;
        if (dbFile != null) {
            BloomFilter fullFilter = new BloomFilter(10.0D, false);
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setFilterPolicy(fullFilter)
                    .setEnableIndexCompression(false)
                    .setIndexBlockRestartInterval(8)
                    .setFormatVersion(5);
            Options options = new Options();
            RocksDBLogger rocksDBLogger = new RocksDBLogger(options);
            options.setCreateIfMissing(false)
                    .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setTableFormatConfig(tableConfig)
                    .setLogger(rocksDBLogger);
            try {
                tmpDb=RocksDB.open(options,dbFile.toString());
            }catch (RocksDBException e){
                if (e.getStatus().getCode() == Status.Code.NotFound) {
                    logger.info("Creating state database at "+dbFile);
                    options.setCreateIfMissing(true);
                    try {
                        tmpDb=RocksDB.open(options,dbFile.toString());
                    }catch (RocksDBException dbExec){
                        throw new IOException("Unable to create state store",dbExec);
                    }
                }else {
                    logger.error("error opening rocksdb file {}. Creating new file, will not be able to " +
                            "recover state for existing applications",dbFile,e);
                    if (dbFile.isDirectory()) {
                        for (File f : Objects.requireNonNull(dbFile.listFiles())) {
                            if (!f.delete()) {
                                logger.warn("error deleting {}",f.getPath());
                            }
                        }
                    }
                    if (!dbFile.delete()) {
                        logger.warn("error deleting {}",dbFile.getPath());
                    }
                    options.setCreateIfMissing(true);
                    try {
                        tmpDb=RocksDB.open(options,dbFile.toString());
                    }catch (RocksDBException dbExec){
                        throw new IOException("Unable to create state store",dbExec);
                    }
                }
            }
            try {
                checkVersion(tmpDb,version,mapper);
            }catch (RocksDBException e){
                throw new IOException(e.getMessage(),e);
            }
        }
        return tmpDb;
    }
    @VisibleForTesting
    static RocksDB initRocksDB(File file) throws IOException {
        BloomFilter fullFilter =
                new BloomFilter(10.0D /* BloomFilter.DEFAULT_BITS_PER_KEY */, false);
        BlockBasedTableConfig tableFormatConfig = new BlockBasedTableConfig()
                .setFilterPolicy(fullFilter)
                .setEnableIndexCompression(false)
                .setIndexBlockRestartInterval(8)
                .setFormatVersion(5);

        Options dbOptions = new Options();
        dbOptions.setCreateIfMissing(true);
        dbOptions.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
        dbOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        dbOptions.setTableFormatConfig(tableFormatConfig);
        try {
            return RocksDB.open(dbOptions, file.toString());
        } catch (RocksDBException e) {
            throw new IOException("Unable to open state store", e);
        }
    }

    private static class RocksDBLogger extends org.rocksdb.Logger{
        private static final Logger LOG=LoggerFactory.getLogger(RocksDBLogger.class);

        public RocksDBLogger(Options options) {
            super(options);
        }

        @Override
        protected void log(InfoLogLevel infoLogLevel, String s) {
            if (infoLogLevel == InfoLogLevel.INFO_LEVEL) {
                logger.info(s);
            }
        }
    }

    public static void checkVersion(RocksDB db, StoreVersion newversion, ObjectMapper mapper) throws
            IOException, RocksDBException {
        byte[] bytes = db.get(StoreVersion.KEY);
        if (bytes == null) {
            storeVersion(db, newversion, mapper);
        } else {
            StoreVersion version = mapper.readValue(bytes, StoreVersion.class);
            if (version.major != newversion.major) {
                throw new IOException("cannot read state DB with version " + version + ", incompatible " +
                        "with current version " + newversion);
            }
            storeVersion(db, newversion, mapper);
        }
    }

    public static void storeVersion(RocksDB db, StoreVersion version, ObjectMapper mapper)
            throws IOException, RocksDBException {
        db.put(StoreVersion.KEY, mapper.writeValueAsBytes(version));
    }
}
