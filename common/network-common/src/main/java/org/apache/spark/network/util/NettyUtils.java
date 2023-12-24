package org.apache.spark.network.util;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.ThreadFactory;

public class NettyUtils {
    private static int MAX_DEFAULT_NETTY_THREADS=8;
    private static final PooledByteBufAllocator[] _sharedPooledByteBufAllocator=new PooledByteBufAllocator[2];
    public static long freeDirectMemory(){
        return PlatformDependent.maxDirectMemory()-PlatformDependent.usedDirectMemory();
    }
    public static ThreadFactory createThreadFactory(String threadPoolPrefix){
        return new DefaultThreadFactory(threadPoolPrefix,true);
    }
    public static EventLoopGroup createEventLoop(IOMode mode,int numThreads,String threadPrefix){
        ThreadFactory threadFactory = createThreadFactory(threadPrefix);
        switch (mode){
            case NIO:
                return new NioEventLoopGroup(numThreads,threadFactory);
            case EPOLL:
                return new EpollEventLoopGroup(numThreads,threadFactory);
            default:
                throw new IllegalArgumentException("Unknown io mode: "+mode);
        }
    }
    public static Class<? extends Channel> getClientChannelClass(IOMode mode){
        switch (mode) {
            case NIO:
                return NioSocketChannel.class;
            case EPOLL:
                return EpollSocketChannel.class;
            default:
                throw new IllegalArgumentException("Unknown io mode"+mode);
        }
    }
    public static Class<? extends ServerChannel> getServerChannelClass(IOMode mode){
        switch (mode){
            case NIO:
                return NioServerSocketChannel.class;
            case EPOLL:
                return EpollServerSocketChannel.class;
            default:
                throw new IllegalArgumentException("Unknown io mode: "+mode);
        }
    }
    public static TransportFrameDecoder createFrameDecoder(){
        return new TransportFrameDecoder();
    }
    public static String getRemoteAddress(Channel channel){
        if (channel != null && channel.remoteAddress() != null) {
            return channel.remoteAddress().toString();
        }
        return "<unknown remote>";
    }
    public static int defaultNumThreads(int numUsableCores){
        final int availableCores;
        if (numUsableCores>0) {
            availableCores=numUsableCores;
        }else {
            availableCores=Runtime.getRuntime().availableProcessors();
        }
        return Math.min(availableCores,MAX_DEFAULT_NETTY_THREADS);
    }
    public static synchronized PooledByteBufAllocator getSharedPooledByteBufAllocator(
            boolean allowDirectBufs,
            boolean allowCache
    ){
        final int index=allowCache?0:1;
        if (_sharedPooledByteBufAllocator[index] == null) {
            _sharedPooledByteBufAllocator[index]=
                    createPooledByteBufAllocator(allowDirectBufs,allowCache,defaultNumThreads(0));
        }
        return _sharedPooledByteBufAllocator[index];
    }
    public static PooledByteBufAllocator createPooledByteBufAllocator(boolean allowDirectBufs, boolean allowCache,int numCores){
        if (numCores==0) {
            numCores=Runtime.getRuntime().availableProcessors();
        }
        return new PooledByteBufAllocator(
                allowDirectBufs&&PlatformDependent.directBufferPreferred(),
                Math.min(PooledByteBufAllocator.defaultNumHeapArena(),numCores),
                Math.min(PooledByteBufAllocator.defaultNumDirectArena(),allowDirectBufs?numCores:0),
                PooledByteBufAllocator.defaultPageSize(),
                PooledByteBufAllocator.defaultMaxOrder(),
                allowCache?PooledByteBufAllocator.defaultSmallCacheSize():0,
                allowCache?PooledByteBufAllocator.defaultNormalCacheSize():0,
                allowCache && PooledByteBufAllocator.defaultUseCacheForAllThreads()
        );
    }
    public static boolean preferDirectBufs(TransportConf conf){
        boolean allowDirectBufs;
        if (conf.sharedByteBufAllocators()) {
            allowDirectBufs=conf.preferDirectBufsForSharedByteBufAllocators();
        }else {
            allowDirectBufs=conf.preferDirectBufs();
        }
        return allowDirectBufs&&PlatformDependent.directBufferPreferred();
    }

}
