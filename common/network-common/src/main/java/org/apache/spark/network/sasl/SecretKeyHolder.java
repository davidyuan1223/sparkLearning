package org.apache.spark.network.sasl;

public interface SecretKeyHolder {
    String getSaslUser(String appId);
    String getSecretKey(String appId);
}
