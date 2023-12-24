package org.apache.spark.network.client;

import com.codahale.metrics.MetricSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class TransportClientFactory implements Closeable {
    private static class ClientPool {
        TransportClient[] clients;
        Object[] locks;
        volatile long lastConnectionFailed;
        ClientPool(int size){
            clients=new TransportClient[size];
            locks=new Object[size];
            for (int i = 0; i < size; i++) {
                locks[i]=new Object();
            }
            lastConnectionFailed=0;
        }
    }
    private static final Logger logger= LoggerFactory.getLogger(TransportClientFactory.class);
    private final TransportContext context;
    private final TransportConf conf;
    private final List<TransportClientBootstrap> clientBootstraps;
    private final ConcurrentHashMap<SocketAddress,ClientPool> connectionPool;
    private final Random rand;
    private final int numConnectionsPerPeer;
    private final Class<? extends Channel> socketChannelClass;
    private EventLoopGroup workerGroup;
    private final PooledByteBufAllocator pooledAllocator;
    private final NettyMemoryMetrics metrics;
    private final int fastFailTimeWindow;
    public TransportClientFactory(TransportContext context,
                                  List<TransportClientBootstrap> clientBootstraps){
        this.context= Preconditions.checkNotNull(context);
        this.conf=context.getConf();
        this.clientBootstraps= Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
        this.connectionPool=new ConcurrentHashMap<>();
        this.numConnectionsPerPeer=conf.numConnectionsPerPeer();
        this.rand=new Random();
        IOMode ioMode = IOMode.valueOf(conf.ioMode());
        this.socketChannelClass= NettyUtils.getClientChannelClass(ioMode);
        this.workerGroup=NettyUtils.createEventLoop(
                ioMode,
                conf.clientThreads(),
                conf.getModuleName()+"-client"
        );
        if (conf.sharedByteBufAllocators()) {
            this.pooledAllocator=NettyUtils.getSharedPooledByteBufAllocator(
                    conf.preferDirectBufsForSharedByteBufAllocators(),false
            );
        }else {
            this.pooledAllocator=NettyUtils.createPooledByteBufAllocator(
                    conf.preferDirectBufs(),false,conf.clientThreads()
            );
        }
        this.metrics=new NettyMemoryMetrics(
                this.pooledAllocator,conf.getModuleName()+"-client",conf
        );
        fastFailTimeWindow=(int) (conf.ioRetryWaitTimeMs()*0.95);
    }

    public MetricSet getAllMetrics(){
        return metrics;
    }
    public TransportClient createClient(String remoteHost,int remotePort,boolean fastFail) throws IOException,InterruptedException {
        final InetSocketAddress unresolvedAddress = InetSocketAddress.createUnresolved(remoteHost,remotePort);
        ClientPool clientPool = connectionPool.computeIfAbsent(unresolvedAddress,
                key -> new ClientPool(numConnectionsPerPeer));
        int clientIndex = rand.nextInt(numConnectionsPerPeer);
        TransportClient cachedClient = clientPool.clients[clientIndex];
        if (cachedClient != null && cachedClient.isActive()) {
            TransportChannelHandler handler = cachedClient.getChannel().pipeline().get(TransportChannelHandler.class);
            synchronized (handler){
                handler.getResponseHandler().updateTimeOfLastRequest();
            }
            if (cachedClient.isActive()) {
                logger.trace("Returning cached connection to {}: {}",
                        cachedClient.getSocketAddress(),cachedClient);
                return cachedClient;
            }
        }
        final long preResolveHost = System.nanoTime();
        final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost,remotePort);
        final long hostResolveTimeMs=(System.nanoTime()-preResolveHost)/1000000;
        final String resolveMsg=resolvedAddress.isUnresolved()?"failed":"succeed";
        if (hostResolveTimeMs > 2000) {
            logger.warn("DNS resolution {} for {} took {} ms",
                    resolveMsg,resolvedAddress,hostResolveTimeMs);
        }else {
            logger.warn("DNS resolution {} for {} took {} ms",
                    resolveMsg,resolvedAddress,hostResolveTimeMs);
        }
        synchronized (clientPool.locks[clientIndex]){
            cachedClient=clientPool.clients[clientIndex];
            if (cachedClient != null) {
                if (cachedClient.isActive()) {
                    logger.trace("Returnning cached connection to {}: {}",resolvedAddress,cachedClient);
                    return cachedClient;
                }else {
                    logger.info("Found inactive connection to {}, creating a new one.",resolvedAddress);
                }
            }
            if (fastFail && System.currentTimeMillis() - clientPool.lastConnectionFailed < fastFailTimeWindow) {
                throw new IOException(
                        String.format("Connecting to %s failed in the last %s ms, fail this connection directly",
                                resolvedAddress,fastFailTimeWindow)
                );
            }
            try {
                clientPool.clients[clientIndex]=createClient(resolvedAddress);
                clientPool.lastConnectionFailed=0;
            }catch (IOException | InterruptedException e){
                clientPool.lastConnectionFailed=System.currentTimeMillis();
                throw e;
            }
            return clientPool.clients[clientIndex];
        }
    }
    public TransportClient createClient(String remoteHost, int remotePort) throws IOException, InterruptedException {
        return createClient(remoteHost,remotePort,false);
    }

    @VisibleForTesting
    TransportClient createClient(InetSocketAddress address) throws IOException, InterruptedException {
        logger.debug("Creating new connection to {}",address);
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(socketChannelClass)
                .option(ChannelOption.TCP_NODELAY,true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,conf.connectionTimeoutMs())
                .option(ChannelOption.ALLOCATOR,pooledAllocator);
        if (conf.receiveBuf()>0) {
            bootstrap.option(ChannelOption.SO_RCVBUF,conf.receiveBuf());
        }
        if (conf.sendBuf()>0) {
            bootstrap.option(ChannelOption.SO_SNDBUF,conf.sendBuf());
        }
        final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
        final AtomicReference<Channel> channelRef = new AtomicReference<>();
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel socketChannel) throws Exception {
                TransportChannelHandler clientHandler = context.initializePipeline(socketChannel);
                clientRef.set(clientHandler.getClient());
                channelRef.set(socketChannel);
            }
        });
        long preConnect = System.nanoTime();
        ChannelFuture cf = bootstrap.connect(address);
        if (!cf.await(conf.connectionCreationTimeoutMs())) {
            throw new IOException(String.format("Connecting to %s timed out (%s ms)",address,conf.connectionCreationTimeoutMs()));
        } else if (cf.cause() != null) {
            throw new IOException(String.format("Failed to connect to %s",address),cf.cause());
        }
        TransportClient client = clientRef.get();
        Channel channel = channelRef.get();
        assert client!=null : "Channel future completed successfully with null client";
        long preBootstrap=System.nanoTime();
        logger.debug("Connection to {} successful, running bootstraps...",address);
        try {
            for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
                clientBootstrap.doBootstrap(client,channel);
            }
        }catch (Exception e){
            long bootstrapTimeMs=(System.nanoTime()-preBootstrap)/1000000;
            logger.error("Exception while bootstrapping client after "+bootstrapTimeMs+" ms",e);
            client.close();
            throw Throwables.propagate(e);
        }
        long postBootstrap = System.nanoTime();
        logger.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
                address,(postBootstrap-preConnect)/1000000,(postBootstrap-preBootstrap)/1000000);
        return client;
    }

    @Override
    public void close() throws IOException {
        for (ClientPool clientPool : connectionPool.values()) {
            for (int i = 0; i < clientPool.clients.length; i++) {
                TransportClient client = clientPool.clients[i];
                if (client != null) {
                    clientPool.clients[i]=null;
                    JavaUtils.closeQuietly(client);
                }
            }
        }
        connectionPool.clear();
        if (workerGroup != null && !workerGroup.isShuttingDown()) {
            workerGroup.shutdownGracefully();
        }
    }
}
