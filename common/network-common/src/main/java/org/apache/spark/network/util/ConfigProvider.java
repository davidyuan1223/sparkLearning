package org.apache.spark.network.util;

import java.util.Map;
import java.util.NoSuchElementException;

public abstract class ConfigProvider {
    public abstract String get(String name);

    public abstract Iterable<Map.Entry<String ,String >> getAll();

    public String get(String name, String defaultValue){
        try {
            return get(name);
        }catch (NoSuchElementException e){
            return defaultValue;
        }
    }

    public int getInt(String name, int defaultValue){
        return Integer.parseInt(get(name,Integer.toString(defaultValue)));
    }

    public long getLone(String name, long defaultValue){
        return Long.parseLong(get(name, Long.toString(defaultValue)));
    }

    public double getDouble(String name, double defaultValue){
        return Double.parseDouble(get(name, Double.toString(defaultValue)));
    }

    public boolean getBoolean(String name, boolean defaultValue){
        return Boolean.getBoolean(get(name, Boolean.toString(defaultValue)));
    }
}
