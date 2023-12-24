package org.apache.spark.network.sasl;

import io.netty.channel.Channel;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.TransportConf;

public class SaslServerBootstrap implements TransportServerBootstrap {
    private final TransportConf conf;
    private final SecretKeyHolder secretKeyHolder;

    public SaslServerBootstrap(TransportConf conf, SecretKeyHolder secretKeyHolder) {
        this.conf = conf;
        this.secretKeyHolder = secretKeyHolder;
    }

    @Override
    public RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler) {
        return new SaslRpcHandler(conf,channel,rpcHandler,secretKeyHolder);
    }
}
