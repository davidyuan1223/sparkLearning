package org.apache.spark.network.crypto;

import io.netty.channel.Channel;
import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.TransportConf;

public class AuthServerBootstrap implements TransportServerBootstrap {
    private final TransportConf conf;
    private final SecretKeyHolder secretKeyHolder;

    public AuthServerBootstrap(TransportConf conf, SecretKeyHolder secretKeyHolder) {
        this.conf = conf;
        this.secretKeyHolder = secretKeyHolder;
    }

    @Override
    public RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler) {
        if (!conf.encryptionEnabled()) {
            SaslServerBootstrap sasl = new SaslServerBootstrap(conf, secretKeyHolder);
            return sasl.doBootstrap(channel,rpcHandler);
        }
        return new AuthRpcHandler(rpcHandler,conf,channel,secretKeyHolder);
    }
}
