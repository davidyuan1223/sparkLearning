package org.apache.spark.network.shuffle;

import com.codahale.metrics.MetricSet;
import com.google.common.collect.Lists;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.MergedBlockMetaResponseCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.crypto.AuthClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.util.TransportConf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class ExternalBlockStoreClient extends BlockStoreClient{
    private static final ErrorHandler PUSH_ERROR_HANDLER = new ErrorHandler.BlockPushErrorHandler();
    private final boolean authEnabled;
    private final SecretKeyHolder secretKeyHolder;
    private final long registrationTimeoutMs;
    private int comparableAppAttemptId=-1;

    public ExternalBlockStoreClient(TransportConf conf,
                                    SecretKeyHolder secretKeyHolder,
                                    boolean authEnabled,
                                    long registrationTimeoutMs){
        this.transportConf=conf;
        this.secretKeyHolder=secretKeyHolder;
        this.authEnabled=authEnabled;
        this.registrationTimeoutMs=registrationTimeoutMs;
    }

    public void init(String appId){
        this.appId=appId;
        TransportContext context = new TransportContext(transportConf, new NoOpRpcHandler(), true, true);
        List<TransportClientBootstrap> bootstraps= Lists.newArrayList();
        if (authEnabled) {
            bootstraps.add(new AuthClientBootstrap(transportConf,appId,secretKeyHolder));
        }
        clientFactory=context.createClientFactory(bootstraps);
    }

    @Override
    public void setAppAttemptId(String appAttemptId) {
        super.setAppAttemptId(appAttemptId);
        setComparableAppAttemptId(appAttemptId);
    }

    private void setComparableAppAttemptId(String appAttemptId){
        try{
            this.comparableAppAttemptId=Integer.parseInt(appAttemptId);
        }catch (NumberFormatException e){
            logger.warn("Push based shuffle requires comparable application attemptId, " +
                    "but the appAttemptId {} cannot be parsed to Integer", appAttemptId, e);
        }
    }

    @Override
    public void fetchBlocks(String host, int port, String execId, String[] blockIds, BlockFetchingListener listener, DownloadFileManager downloadFileManager) {
        checkInit();
        logger.debug("External shuffle fetch from {}:{} (executor id {})",host,port,execId);
        try {
            int maxRetries = transportConf.maxIORetries();
            RetryingBlockTransferor.BlockTransferStarter blockTransferStarter = new RetryingBlockTransferor.BlockTransferStarter() {
                @Override
                public void createAndStart(String[] blockIds, BlockTransferListener listener) throws IOException, InterruptedException {
                    if (clientFactory != null) {
                        assert listener instanceof BlockFetchingListener:
                                "Expecting a BlockFetchingListener, but got "+listener.getClass();
                        TransportClient client = clientFactory.createClient(host, port, maxRetries > 0);
                        new OneForOneBlockFetcher(client,appId,execId,blockIds,
                                (BlockFetchingListener)listener,transportConf,downloadFileManager).start();
                    }else {
                        logger.info("This clientFactory was closed. Skipping further block fetch retries");
                    }
                }
            };
            if (maxRetries>0) {
                new RetryingBlockTransferor(transportConf,blockTransferStarter,blockIds,listener).start();
            }else {
                blockTransferStarter.createAndStart(blockIds,listener);
            }
        }catch (Exception e){
            logger.error("Exception while beginning fetchBlocks",e);
            for (String blockId : blockIds) {
                listener.onBlockFetchFailure(blockId,e);
            }
        }
    }

    @Override
    public void pushBlocks(String host, int port, String[] blockIds, ManagedBuffer[] buffers, BlockPushingListener listener) {
        checkInit();
        assert blockIds.length==buffers.length:"Number of block ids and buffers do not match.";
        Map<String ,ManagedBuffer> buffersWithId = new HashMap<>();
        for (int i = 0; i < blockIds.length; i++) {
            buffersWithId.put(blockIds[i],buffers[i]);
        }
        logger.debug("Push {} shuffle blocks to {}:{}",blockIds.length,host,port);
        try {
            RetryingBlockTransferor.BlockTransferStarter blockPushStarter = new RetryingBlockTransferor.BlockTransferStarter() {
                @Override
                public void createAndStart(String[] blockIds, BlockTransferListener listener) throws IOException, InterruptedException {
                    if (clientFactory != null) {
                        assert listener instanceof BlockPushingListener:
                                "Expecting a BlockPushListener, but got "+listener.getClass();
                        TransportClient client = clientFactory.createClient(host, port);
                        new OneForOneBlockPusher(client,appId,comparableAppAttemptId,blockIds,(BlockPushingListener) listener,buffersWithId).start();
                    }else {
                        logger.info("This clientFactory was closed. Skipping further block push retries");
                    }
                }
            };
            int maxRetries = transportConf.maxIORetries();
            if (maxRetries>0) {
                new RetryingBlockTransferor(transportConf,blockPushStarter,blockIds,listener,PUSH_ERROR_HANDLER).start();
            }else {
                blockPushStarter.createAndStart(blockIds,listener);
            }
        }catch (Exception e){
            logger.error("Exception while beginning pushBlocks",e);
            for (String blockId : blockIds) {
                listener.onBlockPushFailure(blockId,e);
            }
        }
    }

    @Override
    public void finalizeShuffleMerge(String host, int port, int shuffleId, int shuffleMergeId, MergedFinalizeListener listener) {
        checkInit();
        try {
            TransportClient client = clientFactory.createClient(host, port);
            ByteBuffer finalizeShuffleMerge = new FinalizeShuffleMerge(appId, comparableAppAttemptId, shuffleId, shuffleMergeId).toByteBuffer();
            client.sendRpc(finalizeShuffleMerge, new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    listener.onShuffleMergeSuccess((MergeStatuses) BlockTransferMessage.Decoder.fromByteBuffer(response));
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onShuffleMergeFailure(e);
                }
            });
        }catch (Exception e){
            logger.error("Exception while sending finalizeShuffleMerge request to {}:{}",
                    host, port, e);
            listener.onShuffleMergeFailure(e);
        }
    }

    @Override
    public void getMergedBlockMeta(String host, int port, int shuffleId, int shuffleMergeId, int reduceId, MergedBlockMetaListener listener) {
        checkInit();
        logger.debug("Get merged blocks meta from {}:{} for shuffleId {} shuffleMergeId {} reduceId {}",
                host,port,shuffleId,shuffleMergeId,reduceId);
        try {
            TransportClient client = clientFactory.createClient(host, port);
            client.sendMergedBlockMetaReq(appId, shuffleId, shuffleMergeId, reduceId, new MergedBlockMetaResponseCallback() {
                @Override
                public void onSuccess(int numChunks, ManagedBuffer buffer) {
                    logger.trace("Successfully got merged block meta for shuffleId {} shuffleMergeId {} reduceId {}",
                            shuffleId,shuffleMergeId,reduceId);
                    listener.onSuccess(shuffleId,shuffleMergeId,reduceId,new MergedBlockMeta(numChunks,buffer));
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(shuffleId,shuffleMergeId,reduceId,e);
                }
            });
        }catch (Exception e){
            listener.onFailure(shuffleId,shuffleMergeId,reduceId,e);
        }
    }

    @Override
    public boolean removeShuffleMerge(String host, int port, int shuffleId, int shuffleMergeId) {
        checkInit();
        try {
            TransportClient client = clientFactory.createClient(host, port);
            client.send(
                    new RemoveShuffleMerge(appId, comparableAppAttemptId, shuffleId, shuffleMergeId)
                            .toByteBuffer());
            // TODO(SPARK-42025): Add some error logs for RemoveShuffleMerge RPC
        } catch (Exception e) {
            logger.debug("Exception while sending RemoveShuffleMerge request to {}:{}",
                    host, port, e);
            return false;
        }
        return true;
    }
    @Override
    public MetricSet shuffleMetrics() {
        checkInit();
        return clientFactory.getAllMetrics();
    }

    public void registerWithShuffleServer(
            String host,
            int port,
            String execId,
            ExecutorShuffleInfo executorInfo) throws IOException, InterruptedException {
        checkInit();
        try (TransportClient client = clientFactory.createClient(host, port)) {
            ByteBuffer registerMessage = new RegisterExecutor(appId, execId, executorInfo).toByteBuffer();
            client.sendRpcSync(registerMessage, registrationTimeoutMs);
        }
    }

    public Future<Integer> removeBlocks(
            String host,
            int port,
            String execId,
            String[] blockIds) throws IOException, InterruptedException {
        checkInit();
        CompletableFuture<Integer> numRemovedBlocksFuture = new CompletableFuture<>();
        ByteBuffer removeBlocksMessage = new RemoveBlocks(appId, execId, blockIds).toByteBuffer();
        final TransportClient client = clientFactory.createClient(host, port);
        client.sendRpc(removeBlocksMessage, new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                try {
                    BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(response);
                    numRemovedBlocksFuture.complete(((BlocksRemoved) msgObj).numRemovedBlocks);
                } catch (Throwable t) {
                    logger.warn("Error trying to remove blocks " + Arrays.toString(blockIds) +
                            " via external shuffle service from executor: " + execId, t);
                    numRemovedBlocksFuture.complete(0);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                logger.warn("Error trying to remove blocks " + Arrays.toString(blockIds) +
                        " via external shuffle service from executor: " + execId, e);
                numRemovedBlocksFuture.complete(0);
            }
        });
        return numRemovedBlocksFuture;
    }

    @Override
    public void close() throws IOException {
        checkInit();
        if (clientFactory != null) {
            clientFactory.close();
            clientFactory = null;
        }
    }
}
