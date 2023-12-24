package org.apache.spark.network.shuffle;

import org.apache.spark.network.util.JavaUtils;

import java.io.File;

public class ExecutorDiskUtils {
    public static String getFilePath(String[] localDirs,int subDirsPerLocalDir,String filename){
        int hash = JavaUtils.nonNegativeHash(filename);
        String localDir = localDirs[hash % localDirs.length];
        int subDirId = (hash / localDirs.length) % subDirsPerLocalDir;
        final String notNormalizedPath=
                localDir+ File.separator+String.format("%02x",subDirId)+File.separator+filename;
        return new File(notNormalizedPath).getPath().intern();
    }
}
