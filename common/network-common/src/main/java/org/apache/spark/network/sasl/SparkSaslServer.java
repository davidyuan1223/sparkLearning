package org.apache.spark.network.sasl;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.*;
import javax.security.sasl.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SparkSaslServer implements SaslEncryptionBackend{
    private static final Logger logger = LoggerFactory.getLogger(SparkSaslServer.class);
    static final String DEFAULT_REALM="default";
    static final String DIGEST="DIGEST-MD5";
    static final String QOP_AUTH_CONF="auth-conf";
    static final String QOP_AUTH="auth";
    private final String secretKeyId;
    private final SecretKeyHolder secretKeyHolder;
    private SaslServer saslServer;
    public SparkSaslServer(String secretKeyId,SecretKeyHolder secretKeyHolder,boolean alwaysEncrypt){
        this.secretKeyId=secretKeyId;
        this.secretKeyHolder=secretKeyHolder;
        String qop=alwaysEncrypt?QOP_AUTH_CONF:String.format("%s,%s",QOP_AUTH_CONF,QOP_AUTH);
        Map<String ,String > saslProps = ImmutableMap.<String ,String >builder()
                .put(Sasl.SERVER_AUTH,"true")
                .put(Sasl.QOP,qop)
                .build();
        try {
            this.saslServer=Sasl.createSaslServer(DIGEST,null,DEFAULT_REALM,saslProps,new DigestCallbackHandler());
        }catch (SaslException e){
            throw Throwables.propagate(e);
        }
    }

    public synchronized boolean isComplete(){
        return saslServer!=null&&saslServer.isComplete();
    }
    public Object getNegotiateProperty(String name){
        return saslServer.getNegotiatedProperty(name);
    }
    public synchronized byte[] response(byte[] token){
        try {
            return saslServer!=null?saslServer.evaluateResponse(token):new byte[0];
        }catch (SaslException e){
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void dispose() {
        if (saslServer != null) {
            try {
                saslServer.dispose();
            }catch (SaslException e){

            }finally {
                saslServer=null;
            }
        }
    }

    @Override
    public byte[] wrap(byte[] data, int offset, int len) throws SaslException {
        return saslServer.wrap(data,offset,len);
    }

    @Override
    public byte[] unwrap(byte[] data, int offset, int len) throws SaslException {
        return saslServer.unwrap(data,offset,len);
    }

    private class DigestCallbackHandler implements CallbackHandler{
        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    logger.trace("SASL server callback: setting username");
                    NameCallback nc = (NameCallback) callback;
                    nc.setName(encodeIdentifier(secretKeyHolder.getSaslUser(secretKeyId)));
                } else if (callback instanceof PasswordCallback) {
                    logger.trace("SASL server callback: setting password");
                    PasswordCallback pc = (PasswordCallback) callback;
                    pc.setPassword(encodePassword(secretKeyHolder.getSecretKey(secretKeyId)));
                } else if (callback instanceof RealmCallback) {
                    logger.trace("SASL server callback: setting realm");
                    RealmCallback rc= (RealmCallback) callback;
                    rc.setText(rc.getDefaultText());
                } else if (callback instanceof AuthorizeCallback) {
                    AuthorizeCallback ac=(AuthorizeCallback) callback;
                    String authId = ac.getAuthenticationID();
                    String authzId = ac.getAuthorizationID();
                    ac.setAuthorized(authId.equals(authzId));
                    if (ac.isAuthorized()) {
                        ac.setAuthorizedID(authzId);
                    }
                    logger.debug("SASL Authorization complete, authorize set to {}",ac.isAuthorized());
                }else {
                    throw new UnsupportedCallbackException(callback, "Un recognized SASL DIGEST-MD5 Callback");
                }
            }
        }
    }

    public static String encodeIdentifier(String saslUser) {
        Preconditions.checkNotNull(saslUser,"User cannot be null if SASL is enabled");
        return getBase64EncodedString(saslUser);
    }
    public static char[] encodePassword(String password){
        Preconditions.checkNotNull(password,"Password cannot be null if SASL is enabled");
        return getBase64EncodedString(password).toCharArray();
    }
    private static String getBase64EncodedString(String str){
        ByteBuf byteBuf = null;
        ByteBuf encodedByteBuf = null;
        try {
            byteBuf= Unpooled.wrappedBuffer(str.getBytes(StandardCharsets.UTF_8));
            encodedByteBuf= Base64.encode(byteBuf);
            return encodedByteBuf.toString(StandardCharsets.UTF_8);
        }finally {
            if (byteBuf != null) {
                byteBuf.release();
                if (encodedByteBuf != null) {
                    encodedByteBuf.release();
                }
            }
        }
    }
}
