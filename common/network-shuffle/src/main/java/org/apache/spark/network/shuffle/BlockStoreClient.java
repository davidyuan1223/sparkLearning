package org.apache.spark.network.shuffle;

import com.codahale.metrics.MetricSet;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.shuffle.checksum.Cause;
import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class BlockStoreClient implements Closeable {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected volatile TransportClientFactory clientFactory;
    protected String appId;
    private String appAttemptId;
    protected TransportConf transportConf;

    public Cause diagnoseCorruption(String host,int port,String execId,
                                    int shuffleId,long mapId,int reduceId,long checksum,
                                    String algorithm) {
        try {
            TransportClient client = clientFactory.createClient(host, port);
            ByteBuffer resonse = client.sendRpcSync(new DiagnoseCorruption(appId, execId, shuffleId, mapId, reduceId, checksum, algorithm)
                    .toByteBuffer(), transportConf.connectionTimeoutMs());
            CorruptionCause cause = (CorruptionCause) BlockTransferMessage.Decoder.fromByteBuffer(resonse);
            return cause.cause;
        }catch (Exception e){
            logger.warn("Failed to get the corruption cause");
            return Cause.UNKNOWN_ISSUE;
        }
    }

    public abstract void fetchBlocks(String host,int port,String execId,String[] blockIds,
                                     BlockFetchingListener listener,DownloadFileManager downloadFileManager);

    public MetricSet shuffleMetrics(){
        return Collections::emptyMap;
    }

    protected void checkInit(){
        assert appId!=null : "Called before init()";
    }

    public void setAppAttemptId(String appAttemptId) {
        this.appAttemptId = appAttemptId;
    }

    public String getAppAttemptId() {
        return appAttemptId;
    }
    public void getHostLocalDirs(String host, int port, String[] execIds, CompletableFuture<Map<String ,String[]>> hostLocalDirsCompletable){
        checkInit();
        GetLocalDirsForExecutors getLocalDirsMessage = new GetLocalDirsForExecutors(appId, execIds);
        try{
            TransportClient client = clientFactory.createClient(host, port);
            client.sendRpc(getLocalDirsMessage.toByteBuffer(), new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    try {
                        BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(response);
                        hostLocalDirsCompletable.complete(((LocalDirsForExecutors)msgObj).getLocalDirsByExec());
                    }catch (Throwable t) {
                        logger.warn("Error while trying to get the host local dirs for " +
                                Arrays.toString(getLocalDirsMessage.execIds),t.getCause());
                        hostLocalDirsCompletable.completeExceptionally(t);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.warn("Error while trying to get the host local dirs for "+
                            Arrays.toString(getLocalDirsMessage.execIds),e.getCause());
                    hostLocalDirsCompletable.completeExceptionally(e);
                }
            });
        }catch (IndexOutOfBoundsException | InterruptedException | IOException e) {
            hostLocalDirsCompletable.completeExceptionally(e);
        }
    }

    public void pushBlocks(String host, int port, String[] blockIds, ManagedBuffer[] buffers,BlockPushingListener listener){
        throw new UnsupportedOperationException();
    }

    public void finalizeShuffleMerge(String host,int port,int shuffleId,int shuffleMergeId,MergedFinalizeListener listener){
        throw new UnsupportedOperationException();
    }

    public void getMergedBlockMeta(
            String host,
            int port,
            int shuffleId,
            int shuffleMergeId,
            int reduceId,
            MergedBlockMetaListener listener) {
        throw new UnsupportedOperationException();
    }

    /**
     * Remove the shuffle merge data in shuffle services
     *
     * @param host the host of the remote node.
     * @param port the port of the remote node.
     * @param shuffleId shuffle id.
     * @param shuffleMergeId shuffle merge id.
     *
     * @since 3.4.0
     */
    public boolean removeShuffleMerge(String host, int port, int shuffleId, int shuffleMergeId) {
        throw new UnsupportedOperationException();
    }
}
