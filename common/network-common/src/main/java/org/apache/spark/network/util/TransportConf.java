package org.apache.spark.network.util;

import com.google.common.primitives.Ints;
import io.netty.util.NettyRuntime;

import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TransportConf {
    private final String SPARK_NETWORK_IO_MODE_KEY;
    private final String SPARK_NETWORK_IO_PREFERRDIRECTBUFS_KEY;
    private final String SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY;
    private final String SPARK_NETWORK_IO_CONNECTIONCREATIONTIMEOUT_KEY;
    private final String SPARK_NETWORK_IO_BACKLOG_KEY;
    private final String SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY;
    private final String SPARK_NETWORK_IO_SERVERTHREADS_KEY;
    private final String SPARK_NETWORK_IO_CLIENTTHREADS_KEY;
    private final String SPARK_NETWORK_IO_RECEIVERBUFFER_KEY;
    private final String SPARK_NETWORK_IO_SENDBUFFER_KEY;
    private final String SPARK_NETWORK_SASL_TIMEOUT_KEY;
    private final String SPARK_NETWORK_IO_MAXRETRIES_KEY;
    private final String SPARK_NETWORK_IO_RETRYWAIT_KEY;
    private final String SPARK_NETWORK_IO_LAZYFD_KEY;
    private final String SPARK_NETWORK_VERBOSE_METRICS;
    private final String SPARK_NETWORK_IO_ENABLETCPKEEPALIVE_KEY;

    private final ConfigProvider conf;
    private final String module;
    public TransportConf(String module, ConfigProvider conf){
        this.module=module;
        this.conf=conf;
        SPARK_NETWORK_IO_MODE_KEY=getConfKey("io.mode");
        SPARK_NETWORK_IO_PREFERRDIRECTBUFS_KEY=getConfKey("io.preferDirectBufs");
        SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY=getConfKey("io.connectionTimeout");
        SPARK_NETWORK_IO_CONNECTIONCREATIONTIMEOUT_KEY=getConfKey("io.connectionCreationTimeout");
        SPARK_NETWORK_IO_BACKLOG_KEY=getConfKey("io.backLog");
        SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY=getConfKey("io.numConnectionsPerPeer");
        SPARK_NETWORK_IO_SERVERTHREADS_KEY=getConfKey("io.serverThreads");
        SPARK_NETWORK_IO_CLIENTTHREADS_KEY=getConfKey("io.clientThreads");
        SPARK_NETWORK_IO_RECEIVERBUFFER_KEY=getConfKey("io.receiveBuffer");
        SPARK_NETWORK_IO_SENDBUFFER_KEY=getConfKey("io.sendBuffer");
        SPARK_NETWORK_SASL_TIMEOUT_KEY=getConfKey("sasl.timeout");
        SPARK_NETWORK_IO_MAXRETRIES_KEY=getConfKey("io.maxRetries");
        SPARK_NETWORK_IO_RETRYWAIT_KEY=getConfKey("io.retryWait");
        SPARK_NETWORK_IO_LAZYFD_KEY=getConfKey("io.lazyFD");
        SPARK_NETWORK_VERBOSE_METRICS=getConfKey("io.enableVerboseMetrics");
        SPARK_NETWORK_IO_ENABLETCPKEEPALIVE_KEY=getConfKey("io.enableTcpKeepAlive");
    }

    public int getInt(String name, int defaultValue){
        return conf.getInt(name,defaultValue);
    }
    public String get(String name,String defaultValue){
        return conf.get(name,defaultValue);
    }
    public String getConfKey(String suffix){
        return "spark."+module+"."+suffix;
    }
    public String getModuleName(){
        return module;
    }
    public String ioMode(){
        return conf.get(SPARK_NETWORK_IO_MODE_KEY,"NIO").toUpperCase(Locale.ROOT);
    }
    public boolean preferDirectBufs(){
        return conf.getBoolean(SPARK_NETWORK_IO_PREFERRDIRECTBUFS_KEY, true);
    }
    public int connectionTimeoutMs(){
        long defaultNetworkTimeoutS=JavaUtils.timeStringAsSec(
                conf.get("spark.network.timeout","120s")
        );
        long defaultTimeoutMs = JavaUtils.timeStringAsSec(
                conf.get(SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY, defaultNetworkTimeoutS + "s")
        )*1000;
        return (int) defaultTimeoutMs;
    }
    public int connectionCreationTimeoutMs(){
        long connectionTimeoutS = TimeUnit.MILLISECONDS.toSeconds(connectionTimeoutMs());
        long defaultTimeoutMs = JavaUtils.timeStringAsSec(
                conf.get(SPARK_NETWORK_IO_CONNECTIONCREATIONTIMEOUT_KEY, connectionTimeoutS + "s")
        ) * 1000;
        return (int) defaultTimeoutMs;
    }
    public int numConnectionsPerPeer(){
        return conf.getInt(SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY,1);
    }
    public int backLog(){
        return conf.getInt(SPARK_NETWORK_IO_BACKLOG_KEY,-1);
    }
    public int serverThreads(){
        return conf.getInt(SPARK_NETWORK_IO_SERVERTHREADS_KEY,0);
    }
    public int clientThreads(){
        return conf.getInt(SPARK_NETWORK_IO_CLIENTTHREADS_KEY,0);
    }
    public int receiveBuf(){
        return conf.getInt(SPARK_NETWORK_IO_RECEIVERBUFFER_KEY,-1);
    }
    public int sendBuf(){
        return conf.getInt(SPARK_NETWORK_IO_SENDBUFFER_KEY,-1);
    }
    public int authRTTimoutMs(){
        return (int) JavaUtils.timeStringAsSec(conf.get("spark.network.auth.rpcTimeout",
                conf.get(SPARK_NETWORK_SASL_TIMEOUT_KEY,"30s")))*1000;
    }
    public int maxIORetries(){
        return conf.getInt(SPARK_NETWORK_IO_MAXRETRIES_KEY,3);
    }
    public int ioRetryWaitTimeMs(){
        return (int) JavaUtils.timeStringAsSec(conf.get(SPARK_NETWORK_IO_RETRYWAIT_KEY,"5s"))*1000;
    }
    public int memoryMapBytes(){
        return Ints.checkedCast(JavaUtils.byteStringAsBytes(conf.get("spark.storage.memoryMapThreshold","2m")));
    }
    public boolean lazyFileDescriptor(){
        return conf.getBoolean(SPARK_NETWORK_IO_LAZYFD_KEY,true);
    }
    public boolean verboseMetrics(){
        return conf.getBoolean(SPARK_NETWORK_VERBOSE_METRICS,false);
    }
    public boolean enableTcpKeepAlive(){
        return conf.getBoolean(SPARK_NETWORK_IO_ENABLETCPKEEPALIVE_KEY,false);
    }
    public int portMaxRetries(){
        return conf.getInt("spark.port.maxRetries",16);
    }
    public boolean encryptionEnabled(){
        return conf.getBoolean("spark.network.crypto.enabled",false);
    }
    public String cipherTransformation(){
        return conf.get("spark.network.crypto.cipher","AES/CTR/NoPadding");
    }
    public boolean saslFallback(){
        return conf.getBoolean("spark.network.crypto.saslFallback",true);
    }
    public boolean saslEncryption(){
        return conf.getBoolean("spark.authenticate.enableSaslEncryption",false);
    }
    public int maxSaslEncryptedBlockSize(){
        return Ints.checkedCast(JavaUtils.byteStringAsBytes(
                conf.get("spark.network.sasl.maxEncryptedBlockSize","64k")
        ));
    }
    public boolean saslServerAlwaysEncrypt(){
        return conf.getBoolean("spark.network.sasl.serverAlwaysEncrypt",false);
    }
    public boolean sharedByteBufAllocators(){
        return conf.getBoolean("spark.network.sharedByteBufAllocators.enabled",true);
    }
    public boolean preferDirectBufsForSharedByteBufAllocators(){
        return conf.getBoolean("spark.network.io.preferDirectBufs",true);
    }
    public Properties cryptoConf(){
        return CryptoUtils.toCryptoConf("spark.network.crypto.config.",conf.getAll());
    }
    public long maxChunksBeingTransferred(){
        return conf.getLone("spark.shuffle.maxChunksBeingTransferred",Long.MAX_VALUE);
    }
    public int chunkFetchHandlerThreads(){
        if (!this.getModuleName().equalsIgnoreCase("shuffle")) {
            return 0;
        }
        int chunkFetchHandlerThreadsPercent = Integer.parseInt(conf.get("spark.shuffle.server.chunkFetchHandlerThreadsPercent"));
        int threads = this.serverThreads() > 0 ? this.serverThreads() : 2 * NettyRuntime.availableProcessors();
        return (int) Math.ceil(threads*(chunkFetchHandlerThreadsPercent/100.0));
    }
    public boolean separateChunkFetchRequest(){
        return conf.getInt("spark.shuffle.server.chunkFetchHandlerThreadsPercent",0)>0;
    }
    public boolean useOldFetchProtocol(){
        return conf.getBoolean("spark.shuffle.useOldFetchProtocol",false);
    }
    public boolean enableSaslRetries(){
        return conf.getBoolean("spark.shuffle.sasl.enableRetries",false);
    }
    public String mergedShuffleFileManagerImpl(){
        return conf.get("spark.shuffle.push.server.mergedShuffleFileManagerImpl",
                "org.apache.spark.network.shuffle.NoOpMergedShuffleFileManager");
    }
    public int minChunkSizeInMergedShuffleFile(){
        return Ints.checkedCast(JavaUtils.byteStringAsBytes(
                conf.get("spark.shuffle.push.server.minChunkSizeInMergedShuffleFile","2m")
        ));
    }
    public long mergedIndexCacheSize(){
        return JavaUtils.byteStringAsBytes(
                conf.get("spark.shuffle.push.server.mergedCacheSize","100m")
        );
    }
    public int ioExceptionsThresholdDuringMerge(){
        return conf.getInt("spark.shuffle.push.server.ioExceptionsThresholdDuringMerge",4);
    }
    public long mergedShuffleCleanerShutdownTimeout(){
        return JavaUtils.timeStringAsSec(
                conf.get("spark.shuffle.push.server.mergedShuffleCleaner.shutdown.timeout","60s")
        );
    }

}
