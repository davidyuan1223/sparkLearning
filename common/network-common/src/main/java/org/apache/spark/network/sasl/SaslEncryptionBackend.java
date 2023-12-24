package org.apache.spark.network.sasl;

import javax.security.sasl.SaslException;

public interface SaslEncryptionBackend {
    void dispose();
    byte[] wrap(byte[] data, int offset, int len)throws SaslException;
    byte[] unwrap(byte[] data, int offset, int len)throws SaslException;
}
