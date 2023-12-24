package org.apache.spark.network.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.network.shuffledb.StoreVersion;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DataBindingException;
import java.io.File;
import java.io.IOException;

public class LevelDBProvider {
    private static final Logger logger= LoggerFactory.getLogger(LevelDBProvider.class);

    public static DB initLevelDB(File dbFile, StoreVersion version, ObjectMapper mapper)throws IOException{
        DB tmpDb=null;
        if (dbFile != null) {
            Options options = new Options();
            options.createIfMissing(false);
            options.logger(new LevelDBLogger());
            try {
                tmpDb= JniDBFactory.factory.open(dbFile,options);
            }catch (NativeDB.DBException e){
                if (e.isNotFound() || e.getMessage().contains(" does not exist ")){
                    logger.info("Creating state database at "+dbFile);
                    options.createIfMissing(true);
                    try {
                        tmpDb=JniDBFactory.factory.open(dbFile,options);
                    }catch (NativeDB.DBException dbExec){
                        throw new IOException("Unable to create state store", dbExec);
                    }
                }else {
                    logger.error("error opening leveldb file {}. Creating new file, will not be able to " +
                            "recover state for existing applications",dbFile,e);
                    if (dbFile.isDirectory()) {
                        for (File f : dbFile.listFiles()) {
                            if (!f.delete()) {
                                logger.warn("Error deleting {}",f.getAbsolutePath());
                            }
                        }
                    }
                    if (!dbFile.delete()) {
                        logger.warn("error deleting {}",dbFile.getPath());
                    }
                    options.createIfMissing(true);
                    try {
                        tmpDb=JniDBFactory.factory.open(dbFile,options);
                    }catch (DataBindingException dbExec){
                        throw new IOException("Unable to create state store",dbExec);
                    }
                }
            }
            checkVersion(tmpDb,version,mapper);
        }
        return tmpDb;
    }

    @VisibleForTesting
    static DB initLevelDB(File file)throws IOException{
        Options options = new Options();
        options.createIfMissing(true);
        JniDBFactory factory = new JniDBFactory();
        return factory.open(file,options);
    }

    private static class LevelDBLogger implements org.iq80.leveldb.Logger{
        private static final Logger LOG=LoggerFactory.getLogger(LevelDBLogger.class);

        @Override
        public void log(String message) {
            LOG.info(message);
        }
    }

    public static void checkVersion(DB db, StoreVersion newVersion, ObjectMapper mapper)throws IOException{
        byte[] bytes = db.get(StoreVersion.KEY);
        if (bytes == null) {
            storeVersion(db,newVersion,mapper);
        }else {
            StoreVersion version = mapper.readValue(bytes, StoreVersion.class);
            if (version.major != newVersion.major) {
                throw new IOException("cannot read state DB with version " + version + ", incompatible " +
                        "with current version " + newVersion);
            }
            storeVersion(db, newVersion, mapper);
        }
    }

    public static void storeVersion(DB db, StoreVersion version, ObjectMapper mapper)
            throws IOException {
        db.put(StoreVersion.KEY, mapper.writeValueAsBytes(version));
    }
}
