package org.apache.spark;

import org.apache.spark.annotation.Evolving;

@Evolving
public interface QueryContext {
    String objectType();
    String objectName();
    int startIndex();
    int stopIndex();
    String fragment();
}
