package org.apache.spark.network.crypto;

import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.sasl.SaslClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeoutException;

public class AuthClientBootstrap implements TransportClientBootstrap {
    private static final Logger logger = LoggerFactory.getLogger(AuthClientBootstrap.class);
    private final TransportConf conf;
    private final String appId;
    private final SecretKeyHolder secretKeyHolder;
    public AuthClientBootstrap(TransportConf conf,String appId,SecretKeyHolder secretKeyHolder){
        this.conf=conf;
        this.appId=appId;
        this.secretKeyHolder=secretKeyHolder;
    }

    @Override
    public void doBootstrap(TransportClient client, Channel channel) throws RuntimeException {
        if (!conf.encryptionEnabled()) {
            logger.debug("AES encryption disabled, using old auth protocol.");
            doSaslAuth(client,channel);
            return;
        }
        try {
            doSparkAuth(client,channel);
            client.setClientId(appId);
        }catch (GeneralSecurityException | IOException e){
            throw Throwables.propagate(e);
        }catch (RuntimeException e){
            if (!conf.saslFallback() || e.getCause() instanceof TimeoutException) {
                throw e;
            }
            if (logger.isDebugEnabled()) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                logger.debug("New auth protocol failed, trying SASL.",cause);
            }else {
                logger.info("New auth protocol failed, trying SASL.");
            }
            doSaslAuth(client,channel);
        }
    }
    private void doSparkAuth(TransportClient client, Channel channel) throws IOException, GeneralSecurityException {
        String secretKey = secretKeyHolder.getSecretKey(appId);
        try(AuthEngine engine=new AuthEngine(appId,secretKey,conf)){
            AuthMessage challenge = engine.challenge();
            ByteBuf challengeData = Unpooled.buffer(challenge.encodedLength());
            challenge.encode(challengeData);
            ByteBuffer responseData = client.sendRpcSync(challengeData.nioBuffer(), conf.authRTTimoutMs());
            AuthMessage response = AuthMessage.decodeMessage(responseData);
            engine.deriveSessionCipher(challenge,response);
            engine.sessionCipher().addToChannel(channel);
        }
    }

    private void doSaslAuth(TransportClient client,Channel channel){
        SaslClientBootstrap sasl = new SaslClientBootstrap(conf, appId, secretKeyHolder);
        sasl.doBootstrap(client,channel);
    }
}
