package org.apache.spark.network.util;

import java.util.Map;
import java.util.Properties;

public class CryptoUtils {
    public static final String COMMONS_CRYPTO_CONFIG_PREFIX="commons.crypto.";
    public static Properties toCryptoConf(String prefix, Iterable<Map.Entry<String ,String >> conf){
        Properties props = new Properties();
        for (Map.Entry<String, String> e : conf) {
            String key = e.getKey();
            if (key.startsWith(prefix)) {
                props.setProperty(COMMONS_CRYPTO_CONFIG_PREFIX+key.substring(prefix.length()),e.getValue());
            }
        }
        return props;
    }
}
