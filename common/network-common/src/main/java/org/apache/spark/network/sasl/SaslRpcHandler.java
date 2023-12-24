package org.apache.spark.network.sasl;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.crypto.AuthMessage;
import org.apache.spark.network.server.AbstractAuthRpcHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Byte;

import javax.security.sasl.Sasl;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SaslRpcHandler extends AbstractAuthRpcHandler {
    private static final Logger logger = LoggerFactory.getLogger(SaslRpcHandler.class);
    private final TransportConf conf;
    private final Channel channel;
    private final SecretKeyHolder secretKeyHolder;
    private SparkSaslServer saslServer;
    @VisibleForTesting
    SaslRpcHandler saslRpcHandler;
    public SaslRpcHandler(TransportConf conf,
                          Channel channel,
                          RpcHandler delegate,
                          SecretKeyHolder secretKeyHolder){
        super(delegate);
        this.conf=conf;
        this.channel=channel;
        this.secretKeyHolder=secretKeyHolder;
        this.saslServer=null;
    }

    @Override
    public boolean doAuthChallenge(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        if (saslServer == null || !saslServer.isComplete()) {
            ByteBuf nettyBuf = Unpooled.wrappedBuffer(message);
            SaslMessage saslMessage;
            try {
                saslMessage=SaslMessage.decode(nettyBuf);
            }finally {
                nettyBuf.release();
            }
            if (saslServer == null) {
                client.setClientId(saslMessage.appId);
                saslServer=new SparkSaslServer(saslMessage.appId,secretKeyHolder,conf.saslServerAlwaysEncrypt());
            }
            byte[] response;
            try {
                response= saslServer.response(JavaUtils.bufferToArray(saslMessage.body().nioByteBuffer()));
            }catch (IOException e){
                throw new RuntimeException(e);
            }
            callback.onSuccess(ByteBuffer.wrap(response));
        }
        if (saslServer.isComplete()) {
            if (!SparkSaslServer.QOP_AUTH_CONF.equals(saslServer.getNegotiateProperty(Sasl.QOP))){
                logger.debug("SASL authentication successful for channel {}",client);
                complete(true);
                return true;
            }
            logger.debug("Enabling encryption for channel {}",client);
            SaslEncryption.addToChannel(channel,saslServer,conf.maxSaslEncryptedBlockSize());
            complete(false);
            return true;
        }
        return false;
    }

    @Override
    public void channelInActive(TransportClient client) {
        try {
            super.channelInActive(client);
        }finally {
            if (saslServer != null) {
                saslServer.dispose();
            }
        }
    }
    private void complete(boolean dispose){
        if (dispose) {
            try {
                saslServer.dispose();
            }catch (RuntimeException e){
                logger.error("Error while disposing SASL server",e);
            }
        }
        saslServer=null;
    }
}
