package org.apache.spark.network.sasl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

public class SaslClientBootstrap implements TransportClientBootstrap {
    private static final Logger logger = LoggerFactory.getLogger(SaslClientBootstrap.class);
    private final TransportConf conf;
    private final String appId;
    private final SecretKeyHolder secretKeyHolder;

    public SaslClientBootstrap(TransportConf conf, String appId, SecretKeyHolder secretKeyHolder) {
        this.conf = conf;
        this.appId = appId;
        this.secretKeyHolder = secretKeyHolder;
    }

    @Override
    public void doBootstrap(TransportClient client, Channel channel) throws RuntimeException {
        SparkSaslClient saslClient = new SparkSaslClient(appId, secretKeyHolder, conf.saslEncryption());
        try {
            byte[] payload = saslClient.firstToken();
            while (!saslClient.isComplete()) {
                SaslMessage msg = new SaslMessage(appId, payload);
                ByteBuf buf = Unpooled.buffer(msg.encodedLength() + (int) msg.body().size());
                msg.encode(buf);
                ByteBuffer response;
                buf.writeBytes(msg.body().nioByteBuffer());
                try {
                    response=client.sendRpcSync(buf.nioBuffer(),conf.authRTTimoutMs());
                }catch (RuntimeException e){
                    if (e.getCause() instanceof TimeoutException) {
                        throw new SaslTimeoutException(e.getCause());
                    }else {
                        throw e;
                    }
                }
                payload=saslClient.response(JavaUtils.bufferToArray(response));
            }
            client.setClientId(appId);
            if (conf.saslEncryption()) {
                if (!SparkSaslServer.QOP_AUTH_CONF.equals(saslClient.getNegotiatedProperty(Sasl.QOP))) {
                    throw new RuntimeException(new SaslException("Encryption requests by negotiated non-encrypted connection."));
                }
                SaslEncryption.addToChannel(channel,saslClient,conf.maxSaslEncryptedBlockSize());
                saslClient=null;
                logger.debug("Channel {} configured for encryption.",client);
            }
        }catch (IOException e){
            throw new RuntimeException(e);
        }finally {
            if (saslClient != null) {
                try {
                    saslClient.dispose();
                }catch (RuntimeException e){
                    logger.error("Error while disposing SASL client",e);
                }
            }
        }
    }
}
