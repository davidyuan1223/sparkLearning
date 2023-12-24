package org.apache.spark.network.sasl;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.spark.network.sasl.SparkSaslServer.*;

import javax.security.auth.callback.*;
import javax.security.sasl.*;
import java.io.IOException;
import java.util.Map;

public class SparkSaslClient implements SaslEncryptionBackend{
    private static final Logger logger = LoggerFactory.getLogger(SparkSaslClient.class);
    private final String secretKeyId;
    private final SecretKeyHolder secretKeyHolder;
    private final String expectedQop;
    private SaslClient saslClient;

    public SparkSaslClient(String secretKeyId, SecretKeyHolder secretKeyHolder, boolean encrypt) {
        this.secretKeyId = secretKeyId;
        this.secretKeyHolder = secretKeyHolder;
        this.expectedQop = encrypt?QOP_AUTH_CONF:QOP_AUTH;
        Map<String ,String > saslProps = ImmutableMap.<String ,String >builder()
                .put(Sasl.QOP,expectedQop)
                .build();
        try {
            this.saslClient=Sasl.createSaslClient(new String[] {DIGEST},null,null,DEFAULT_REALM,
                    saslProps,new ClientCallbackHandler());
        }catch (SaslException e){
            throw Throwables.propagate(e);
        }
    }

    public synchronized byte[] firstToken(){
        if (saslClient != null && saslClient.hasInitialResponse()) {
            try {
                return saslClient.evaluateChallenge(new byte[0]);
            }catch (SaslException e){
                throw Throwables.propagate(e);
            }
        }else {
            return new byte[0];
        }
    }

    public synchronized boolean isComplete(){
        return saslClient!=null && saslClient.isComplete();
    }

    public Object getNegotiatedProperty(String name){
        return saslClient.getNegotiatedProperty(name);
    }

    public synchronized byte[] response(byte[] token){
        try {
            return saslClient!=null ? saslClient.evaluateChallenge(token):new byte[0];
        }catch (SaslException e){
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void dispose() {
        if (saslClient != null) {
            try {
                saslClient.dispose();
            }catch (SaslException e){

            }finally {
                saslClient=null;
            }
        }
    }

    private class ClientCallbackHandler implements CallbackHandler{
        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    logger.trace("SASL client callback: setting username");
                    NameCallback nc = (NameCallback) callback;
                    nc.setName(encodeIdentifier(secretKeyHolder.getSaslUser(secretKeyId)));
                } else if (callback instanceof PasswordCallback) {
                    logger.trace("SASL client callback: setting password");
                    PasswordCallback pc = (PasswordCallback) callback;
                    pc.setPassword(encodePassword(secretKeyHolder.getSecretKey(secretKeyId)));
                } else if (callback instanceof RealmCallback) {
                    logger.trace("SASL client callback: setting realm");
                    RealmCallback rc=(RealmCallback) callback;
                    rc.setText(rc.getDefaultText());
                } else if (callback instanceof RealmChoiceCallback) {

                }else {
                    throw new UnsupportedCallbackException(callback,"Unrecognized SASL DIGEST-MD5 Callback");
                }
            }
        }
    }

    @Override
    public byte[] wrap(byte[] data, int offset, int len) throws SaslException {
        return saslClient.wrap(data,offset,len);
    }

    @Override
    public byte[] unwrap(byte[] data, int offset, int len) throws SaslException {
        return saslClient.unwrap(data,offset,len);
    }
}
