package org.apache.spark.network.shuffle;

import org.apache.spark.network.buffer.ManagedBuffer;

import java.nio.channels.WritableByteChannel;

public interface DownloadFileWritableChannel extends WritableByteChannel {
    ManagedBuffer closeAndRead();
}
