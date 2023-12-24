package org.apache.spark.network.shuffledb;

import org.apache.spark.annotation.Private;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;

@Private
public interface DBIterator extends Iterator<Map.Entry<byte[],byte[]>>, Closeable {
    void seek(byte[] key);
    default void remove(){
        throw new UnsupportedOperationException();
    }
}
