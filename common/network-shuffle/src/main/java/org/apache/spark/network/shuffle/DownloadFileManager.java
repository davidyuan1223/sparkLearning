package org.apache.spark.network.shuffle;

import org.apache.spark.network.util.TransportConf;

public interface DownloadFileManager {
    DownloadFile createTempFile(TransportConf transportConf);
    boolean registerTempFileToClean(DownloadFile file);
}
