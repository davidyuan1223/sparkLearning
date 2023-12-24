package org.apache.spark.network.shuffledb;

import org.apache.spark.annotation.Private;

import java.io.Closeable;
@Private
public interface DB extends Closeable {
    void put(byte[] key, byte[] value);
    byte[] get(byte[] key);
    void delete(byte[] key);
    DBIterator iterator();
}
