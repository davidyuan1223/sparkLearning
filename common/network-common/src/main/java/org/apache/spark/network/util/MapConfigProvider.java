package org.apache.spark.network.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class MapConfigProvider extends ConfigProvider{
    public static final MapConfigProvider EMPTY = new MapConfigProvider(Collections.emptyMap());
    private final Map<String ,String > config;

    public MapConfigProvider(Map<String ,String > config){
        this.config=new HashMap<>(config);
    }

    @Override
    public String get(String name) {
        String value = config.get(name);
        if (value == null) {
            throw new NoSuchElementException(name);
        }
        return value;
    }

    @Override
    public String get(String name, String defaultValue) {
        String value = config.get(name);
        return value==null ? defaultValue:value;
    }

    @Override
    public Iterable<Map.Entry<String, String>> getAll() {
        return config.entrySet();
    }
}
