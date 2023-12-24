package org.apache.spark.network.shuffle;

import java.io.IOException;

public interface DownloadFile {
    boolean delete();
    DownloadFileWritableChannel openForWriting() throws IOException;
    String path();
}
