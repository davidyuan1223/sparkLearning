package org.apache.spark;

import org.apache.spark.annotation.Evolving;
import java.util.HashMap;
import java.util.Map;

@Evolving
public interface SparkThrowable {
    String getErrorClass();
    default String getSqlState(){
        return SparkThrowableHelper.getSqlState(this.getErrorClass());
    }
    default boolean isInternalError(){
        return SparkThrowableHelper.isInternalError(this.getErrorClass());
    }
    default Map<String ,String > getMessageParameters(){
        return new HashMap<>();
    }
    default QueryContext[] getQueryContext(){
        return new QueryContext[0];
    }
}
