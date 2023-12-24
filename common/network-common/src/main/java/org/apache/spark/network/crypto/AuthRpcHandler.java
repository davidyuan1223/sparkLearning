package org.apache.spark.network.crypto;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.sasl.SaslRpcHandler;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.AbstractAuthRpcHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class AuthRpcHandler extends AbstractAuthRpcHandler {
    private static final Logger LOG= LoggerFactory.getLogger(AuthRpcHandler.class);
    private final TransportConf conf;
    private final Channel channel;
    private final SecretKeyHolder secretKeyHolder;
    @VisibleForTesting
    SaslRpcHandler saslHandler;

    public AuthRpcHandler(RpcHandler delegate, TransportConf conf, Channel channel, SecretKeyHolder secretKeyHolder) {
        super(delegate);
        this.conf = conf;
        this.channel = channel;
        this.secretKeyHolder = secretKeyHolder;
    }

    @Override
    protected boolean doAuthChallenge(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        if (saslHandler != null) {
            return saslHandler.doAuthChallenge(client,message,callback);
        }
        int position = message.position();
        int limit = message.limit();
        AuthMessage challenge;
        try {
            challenge=AuthMessage.decodeMessage(message);
            LOG.debug("Received new auth challenge for client {}.",channel.remoteAddress());
        }catch (RuntimeException e){
            if (conf.saslFallback()) {
                LOG.warn("Failed to parse new auth challenge, reverting to SASL for client {}.",channel.remoteAddress());
                saslHandler=new SaslRpcHandler(conf,channel,null,secretKeyHolder);
                message.position(position);
                message.limit(limit);
                return saslHandler.doAuthChallenge(client,message,callback);
            }else {
                LOG.debug("Unexpected challenge message from client {}, closing channel.",channel.remoteAddress());
                callback.onFailure(new IllegalArgumentException("Unknown challenge message"));
                channel.close();
            }
            return false;
        }
        AuthEngine engine=null;
        try {
            String secret = secretKeyHolder.getSecretKey(challenge.appId);
            Preconditions.checkState(secret!=null,
                    "Trying to authenticate non-registered app %s.",challenge);
            LOG.debug("Authenticating challenge for app {}.",challenge.appId);
            engine=new AuthEngine(challenge.appId,secret,conf);
            AuthMessage response = engine.response(challenge);
            ByteBuf responseData = Unpooled.buffer(response.encodedLength());
            response.encode(responseData);
            callback.onSuccess(responseData.nioBuffer());
            engine.sessionCipher().addToChannel(channel);
            client.setClientId(challenge.appId);
        }catch (Exception e){
            LOG.debug("Authentication failed for client{}, closing channel.",channel.remoteAddress());
            callback.onFailure(new IllegalArgumentException("Authentication failed"));
            channel.close();
            return false;
        }finally {
            if (engine != null) {
                try {
                    engine.close();
                }catch (Exception e){
                    throw Throwables.propagate(e);
                }
            }
        }
        LOG.debug("Authorization successful for client {}.",channel.remoteAddress());
        return true;
    }

    @Override
    public MergedBlockMetaReqHandler getMergedBlockMetaReqHandler() {
        return saslHandler.getMergedBlockMetaReqHandler();
    }
}
