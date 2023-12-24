package org.apache.spark.network.shuffle;

import com.codahale.metrics.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.MergedBlockMetaResponseCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.MergedBlockMetaRequest;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.shuffle.checksum.Cause;
import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.util.TimerWithCustomTimeUnit;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

public class ExternalBlockHandler extends RpcHandler implements RpcHandler.MergedBlockMetaReqHandler {
    private static final Logger logger= LoggerFactory.getLogger(ExternalBlockHandler.class);
    private static final String SHUFFLE_MERGE_IDENTIFIER="shuffle-push-merger";
    private static final String SHUFFLE_BLOCK_ID="shuffle";
    private static final String SHUFFLE_CHUNK_ID="shuffleChunk";
    @VisibleForTesting
    final ExternalShuffleBlockResolver blockManager;
    private final OneForOneStreamManager streamManager;
    private final ShuffleMetrics metrics;
    private final MergedShuffleFileManager mergeManager;
    public ExternalBlockHandler(TransportConf conf, File registeredExecutorFile)throws IOException{
        this(new OneForOneStreamManager(),new ExternalShuffleBlockResolver(conf,registeredExecutorFile),
                new NoOpMergedShuffleFileManager(conf,null));
    }

    public ExternalBlockHandler(TransportConf conf,File registeredExecutorFile,MergedShuffleFileManager mergeManager)throws IOException{
        this(new OneForOneStreamManager(),
                new ExternalShuffleBlockResolver(conf,registeredExecutorFile),
                mergeManager);
    }

    @VisibleForTesting
    public ExternalShuffleBlockResolver getBlockResolver(){
        return blockManager;
    }

    @VisibleForTesting
    public ExternalBlockHandler(OneForOneStreamManager streamManager,ExternalShuffleBlockResolver blockManager){
        this(streamManager,blockManager,new NoOpMergedShuffleFileManager(null,null));
    }

    @VisibleForTesting
    public ExternalBlockHandler(OneForOneStreamManager streamManager,
                                ExternalShuffleBlockResolver blockManager,
                                MergedShuffleFileManager mergeManager){
        this.metrics=new ShuffleMetrics();
        this.streamManager=streamManager;
        this.blockManager=blockManager;
        this.mergeManager=mergeManager;
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message);
        handleMessage(msgObj,client,callback);
    }

    @Override
    public StreamCallbackWithID receiveStream(TransportClient client, ByteBuffer messageHeader, RpcResponseCallback callback) {
        BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(messageHeader);
        if (msgObj instanceof PushBlockStream){
            PushBlockStream message = (PushBlockStream) msgObj;
            checkAuth(client,message.appId);
            return mergeManager.receiveBlockDataAsStream(message);
        }else {
            throw new UnsupportedOperationException("Unexpected message with #receiveStream: "+msgObj);
        }
    }

    protected void handleMessage(BlockTransferMessage msgObj,TransportClient client,RpcResponseCallback callback){
        if (msgObj instanceof AbstractFetchShuffleBlocks || msgObj instanceof OpenBlocks){
            final Timer.Context responseDelayContext = metrics.openBlockRequestLatencyMillis.time();
            try {
                int numBlockIds;
                long streamId;
                if (msgObj instanceof AbstractFetchShuffleBlocks) {
                    AbstractFetchShuffleBlocks msg = (AbstractFetchShuffleBlocks) msgObj;
                    checkAuth(client,msg.appId);
                    numBlockIds=((AbstractFetchShuffleBlocks)msgObj).getNumBlocks();
                    Iterator<ManagedBuffer> iterator;
                    if (msgObj instanceof FetchShuffleBlocks){
                        iterator=new ShuffleManagedBufferIterator((FetchShuffleBlocks)msgObj);
                    }else {
                        iterator = new ShuffleChunkManagedBufferIterator((FetchShuffleBlockChunks)msgObj);
                    }
                    streamId=streamManager.registerStream(client.getClientId(),iterator,client.getChannel(),true);
                }else {
                    OpenBlocks msg = (OpenBlocks) msgObj;
                    numBlockIds=msg.blockIds.length;
                    checkAuth(client,msg.appId);
                    streamId=streamManager.registerStream(client.getClientId(),
                            new ManagedBufferIterator(msg),client.getChannel(),true);
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("Registered streamId {} with {} buffers for client {} from host {}",
                            streamId,numBlockIds,client.getClientId(),getRemoteAddress(client.getChannel()));
                }
                callback.onSuccess(new StreamHandle(streamId,numBlockIds).toByteBuffer());
            }finally {
                responseDelayContext.stop();
            }
        }else if (msgObj instanceof RegisterExecutor){
            final Timer.Context responseDelayContext = metrics.registerExecutorRequestLatencyMillis.time();
            try {
                RegisterExecutor msg = (RegisterExecutor) msgObj;
                checkAuth(client,msg.appId);
                blockManager.registerExecutor(msg.appId,msg.execId,msg.executorInfo);
                mergeManager.registerExecutor(msg.appId,msg.executorInfo);
                callback.onSuccess(ByteBuffer.wrap(new byte[0]));
            }finally {
                responseDelayContext.stop();
            }
        } else if (msgObj instanceof RemoveBlocks) {
            RemoveBlocks msg = (RemoveBlocks) msgObj;
            checkAuth(client,msg.appId);
            int numRemoveBlocks = blockManager.removeBlocks(msg.appId, msg.execId, msg.blockIds);
            callback.onSuccess(new BlocksRemoved(numRemoveBlocks).toByteBuffer());
        }else if (msgObj instanceof GetLocalDirsForExecutors){
            GetLocalDirsForExecutors msg = (GetLocalDirsForExecutors) msgObj;
            checkAuth(client,msg.appId);
            HashSet<String> execIdsForBlockResolver = Sets.newHashSet(msg.execIds);
            boolean fetchMergedBlockDirs = execIdsForBlockResolver.remove(SHUFFLE_MERGE_IDENTIFIER);
            Map<String, String[]> localDirs = blockManager.getLocalDirs(msg.appId, execIdsForBlockResolver);
            if (fetchMergedBlockDirs) {
                localDirs.put(SHUFFLE_MERGE_IDENTIFIER,mergeManager.getMergedBlockDirs(msg.appId));
            }
            callback.onSuccess(new LocalDirsForExecutors(localDirs).toByteBuffer());
        }else if (msgObj instanceof FinalizeShuffleMerge){
            final Timer.Context responseDelayContext = metrics.finalizeShuffleMergeLatencyMillis.time();
            FinalizeShuffleMerge msg = (FinalizeShuffleMerge) msgObj;
            try {
                checkAuth(client,msg.appId);
                MergeStatuses statuses = mergeManager.finalizeShuffleMerge(msg);
                callback.onSuccess(statuses.toByteBuffer());
            }catch (IOException e){
                throw new RuntimeException(String.format("Error while finalizing shuffle merge for application %s shuflle %d with shuffleMergeId %d",msg.appId,
                        msg.shuffleId,msg.shuffleMergeId));
            }finally {
                responseDelayContext.stop();
            }
        }else if (msgObj instanceof RemoveShuffleMerge){
            RemoveShuffleMerge msg = (RemoveShuffleMerge) msgObj;
            checkAuth(client,msg.appId);
            logger.info("Removing shuffle merge data for application {} shuffle {} shuffleMerge {}",
                    msg.appId,msg.shuffleId,msg.shuffleMergeId);
            mergeManager.removeShuffleMerge(msg);
        } else if (msgObj instanceof DiagnoseCorruption) {
            DiagnoseCorruption msg = (DiagnoseCorruption) msgObj;
            checkAuth(client,msg.appId);
            Cause cause = blockManager.diagnoseShuffleBlockCorruption(msg.appId, msg.execId, msg.shuffleId, msg.mapId, msg.reduceId, msg.checksum, msg.algorithm);
            callback.onSuccess(new CorruptionCause(cause).toByteBuffer());
        }else {
            throw new UnsupportedOperationException("Unexpected message: "+msgObj);
        }
    }

    @Override
    public void receiveMergedBlockMetaReq(TransportClient client, MergedBlockMetaRequest mergedBlockMetaRequest, MergedBlockMetaResponseCallback callback) {
        final Timer.Context responseDelayContext = metrics.fetchMergedBlocksMetaLatencyMillis.time();
        try {
            checkAuth(client,mergedBlockMetaRequest.appId);
            MergedBlockMeta mergedMeta = mergeManager.getMergedBlockMeta(mergedBlockMetaRequest.appId, mergedBlockMetaRequest.shuffleId,
                    mergedBlockMetaRequest.shuffleMergeId, mergedBlockMetaRequest.reduceId);
            logger.debug("Merged block chunks appId {} shuffleId {} reduceId {} num-chunks: {} ",
                    mergedBlockMetaRequest.appId,mergedBlockMetaRequest.shuffleId,mergedBlockMetaRequest.reduceId,mergedMeta.getNumChunks());
            callback.onSuccess(mergedMeta.getNumChunks(),mergedMeta.getChunksBitmapBuffer());
        }finally {
            responseDelayContext.stop();
        }
    }

    @Override
    public MergedBlockMetaReqHandler getMergedBlockMetaReqHandler() {
        return this;
    }

    @Override
    public void exceptionCaught(Throwable cause, TransportClient client) {
        metrics.caughtExceptions.inc();
    }

    public ShuffleMetrics getAllMetrics() {
        return metrics;
    }

    @Override
    public OneForOneStreamManager getStreamManager() {
        return streamManager;
    }

    public void applicationRemoved(String appId,boolean cleanupLocalDirs){
        blockManager.applicationRemove(appId,cleanupLocalDirs);
        mergeManager.applicationRemoved(appId,cleanupLocalDirs);
    }

    public void executorRemoved(String executorId,String appId){
        blockManager.executorRemoved(executorId,appId);
    }
    public void close(){
        blockManager.close();
        mergeManager.close();
    }

    private void checkAuth(TransportClient client, String appId){
        if (client.getClientId() != null && !client.getClientId().equals(appId)) {
            throw new SecurityException(String.format("Client for %s not authorized for application %s.",client.getClientId(),appId));
        }
    }

    @VisibleForTesting
    public class ShuffleMetrics implements MetricSet {
        private final Map<String , Metric> allMetrics;
        private final Timer openBlockRequestLatencyMillis= new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);
        private final Timer registerExecutorRequestLatencyMillis = new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);
        private final Timer fetchMergedBlocksMetaLatencyMillis =
                new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);
        // Time latency for processing finalize shuffle merge request latency in ms
        private final Timer finalizeShuffleMergeLatencyMillis =
                new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);
        // Block transfer rate in blocks per second
        private final Meter blockTransferRate = new Meter();
        // Block fetch message rate per second. When using non-batch fetches
        // (`OpenBlocks` or `FetchShuffleBlocks` with `batchFetchEnabled` as false), this will be the
        // same as the `blockTransferRate`. When batch fetches are enabled, this will represent the
        // number of batch fetches, and `blockTransferRate` will represent the number of blocks
        // returned by the fetches.
        private final Meter blockTransferMessageRate = new Meter();
        // Block transfer rate in byte per second
        private final Meter blockTransferRateBytes = new Meter();
        // Number of active connections to the shuffle service
        private Counter activeConnections = new Counter();
        // Number of exceptions caught in connections to the shuffle service
        private Counter caughtExceptions = new Counter();
        public ShuffleMetrics(){
            allMetrics=new HashMap<>();
            allMetrics.put("openBlockRequestLatencyMillis",openBlockRequestLatencyMillis);
            allMetrics.put("registerExecutorRequestLatencyMillis", registerExecutorRequestLatencyMillis);
            allMetrics.put("fetchMergedBlocksMetaLatencyMillis", fetchMergedBlocksMetaLatencyMillis);
            allMetrics.put("finalizeShuffleMergeLatencyMillis", finalizeShuffleMergeLatencyMillis);
            allMetrics.put("blockTransferRate", blockTransferRate);
            allMetrics.put("blockTransferMessageRate", blockTransferMessageRate);
            allMetrics.put("blockTransferRateBytes", blockTransferRateBytes);
            allMetrics.put("blockTransferAvgSize_1min", new RatioGauge() {
                @Override
                protected Ratio getRatio() {
                    return Ratio.of(
                            blockTransferRateBytes.getOneMinuteRate(),
                            // use blockTransferMessageRate here instead of blockTransferRate to represent the
                            // average size of the disk read / network message which has more operational impact
                            // than the actual size of the block
                            blockTransferMessageRate.getOneMinuteRate());
                }
            });
            allMetrics.put("registeredExecutorsSize",
                    (Gauge<Integer>) () -> blockManager.getRegisteredExecutorsSize());
            allMetrics.put("numActiveConnections", activeConnections);
            allMetrics.put("numCaughtExceptions", caughtExceptions);
        }

        public Map<String, Metric> getMetrics() {
            return allMetrics;
        }
    }

    private class ShuffleManagedBufferIterator implements Iterator<ManagedBuffer> {
        private int mapIdx=0;
        private int reduceIdx=0;
        private final String appId;
        private final String execId;
        private final int shuffleId;
        private final long[] mapIds;
        private final int[][] reduceIds;
        private final boolean batchFetchEnabled;
        ShuffleManagedBufferIterator(FetchShuffleBlocks msg){
            appId=msg.appId;
            execId=msg.execId;
            shuffleId=msg.shuffleId;
            mapIds=msg.mapIds;
            reduceIds=msg.reduceIds;
            batchFetchEnabled=msg.batchFetchEnabled;
            assert mapIds.length!=0 && mapIds.length==reduceIds.length;
        }

        @Override
        public boolean hasNext() {
            return mapIdx<mapIds.length && reduceIdx<reduceIds[mapIdx].length;
        }

        @Override
        public ManagedBuffer next() {
            ManagedBuffer block;
            if (!batchFetchEnabled) {
                block=blockManager.getBlockData(appId,execId,shuffleId,mapIds[mapIdx],reduceIds[mapIdx][reduceIdx]);
                if (reduceIdx < reduceIds[mapIdx].length - 1) {
                    reduceIdx+=1;
                }else {
                    reduceIdx=0;
                    mapIdx+=1;
                }
                metrics.blockTransferRate.mark();
            }else {
                assert reduceIds[mapIdx].length==2;
                int startReduceId = reduceIds[mapIdx][0];
                int endReduceId = reduceIds[mapIdx][1];
                block=blockManager.getContinuousBlocksData(appId,execId,shuffleId,mapIds[mapIdx],startReduceId,endReduceId);
                mapIdx+=1;
                metrics.blockTransferRate.mark(endReduceId-startReduceId);
            }
            metrics.blockTransferMessageRate.mark();
            metrics.blockTransferRateBytes.mark(block!=null ? block.size():0);
            return block;
        }
    }

    private class ShuffleChunkManagedBufferIterator implements Iterator<ManagedBuffer> {
        private int reduceIdx=0;
        private int chunkIdx=0;
        private final String appId;
        private final int shuffleId;
        private final int shuffleMergeId;
        private final int[] reduceIds;
        private final int[][] chunkIds;

        ShuffleChunkManagedBufferIterator(FetchShuffleBlockChunks msg){
            appId=msg.appId;
            shuffleId=msg.shuffleId;
            shuffleMergeId=msg.shuffleMergeId;
            reduceIds=msg.reduceIds;
            chunkIds=msg.chunkIds;
            assert reduceIds.length!=0 && reduceIds.length==chunkIds.length;
        }

        @Override
        public boolean hasNext() {
            return reduceIdx<reduceIds.length && chunkIdx<chunkIds[reduceIdx].length;
        }

        @Override
        public ManagedBuffer next() {
            ManagedBuffer block= Preconditions.checkNotNull(mergeManager.getMergedBlockData(
                    appId,shuffleId,shuffleMergeId,reduceIds[reduceIdx],chunkIds[reduceIdx][chunkIdx]
            ));
            if (chunkIdx<chunkIds[reduceIdx].length-1){
                chunkIdx+=1;
            }else {
                chunkIdx=0;
                reduceIdx+=1;
            }
            metrics.blockTransferRateBytes.mark(block.size());
            return block;
        }
    }

    private class ManagedBufferIterator implements Iterator<ManagedBuffer> {
        private int index=0;
        private final Function<Integer,ManagedBuffer> blockDataForIndexFn;
        private final int size;
        ManagedBufferIterator(OpenBlocks msg){
            String appId=msg.appId;
            String execId=msg.execId;
            String[] blocksIds=msg.blockIds;
            String[] blockId0Parts=blocksIds[0].split("_");
            if (blockId0Parts.length == 4 && blockId0Parts[0].equals(SHUFFLE_BLOCK_ID)) {
                final int shuffleId = Integer.parseInt(blockId0Parts[1]);
                final int[] mapIdAndReduceIds = shuffleMapIdAndReduceIds(blocksIds,shuffleId);
                size=mapIdAndReduceIds.length;
                blockDataForIndexFn=index->blockManager.getBlockData(appId,execId,shuffleId,mapIdAndReduceIds[index],
                        mapIdAndReduceIds[index+1]);
            } else if (blockId0Parts.length == 5 && blockId0Parts[0].equals(SHUFFLE_CHUNK_ID)) {
                final int shuffleId = Integer.parseInt(blockId0Parts[1]);
                final int shuffleMergeId = Integer.parseInt(blockId0Parts[2]);
                final int[] reduceIdAndChunkIds = shuffleReduceIdAndChunkIds(blocksIds,shuffleId,shuffleMergeId);
                size=reduceIdAndChunkIds.length;
                blockDataForIndexFn=index->mergeManager.getMergedBlockData(msg.appId,shuffleId,shuffleMergeId,reduceIdAndChunkIds[index],reduceIdAndChunkIds[index+1]);
            }else if (blockId0Parts.length==3 && blockId0Parts[0].equals("rdd")){
                final int[] rddAndSplitIds = rddAndSplitIds(blocksIds);
                size=rddAndSplitIds.length;
                blockDataForIndexFn=index->blockManager.getRddBlockData(appId,execId,rddAndSplitIds[index],rddAndSplitIds[index+1]);
            }else {
                throw new IllegalArgumentException("Unexpected block id format: "+blocksIds[0]);
            }
        }
        private int[] rddAndSplitIds(String [] blockIds){
            final int[] rddAndSplitIds = new int[2*blockIds.length];
            for (int i = 0; i < blockIds.length; i++) {
                String[] blockIdParts = blockIds[i].split("_");
                if (blockIdParts.length!=3 || !blockIdParts[0].equals("rdd")){
                    throw new IllegalArgumentException("Unexpected RDD block id format: "+blockIds[i]);
                }
                rddAndSplitIds[2*i]=Integer.parseInt(blockIdParts[1]);
                rddAndSplitIds[2*i+1]=Integer.parseInt(blockIdParts[2]);
            }
            return rddAndSplitIds;
        }
        private int[] shuffleMapIdAndReduceIds(String[] blockIds, int shuffleId){
            final int[] mapIdAndReduceIds = new int[2*blockIds.length];
            for (int i = 0; i < blockIds.length; i++) {
                String[] blockIdParts = blockIds[i].split("_");
                if (blockIdParts.length != 4 || !blockIdParts[0].equals(SHUFFLE_BLOCK_ID)) {
                    throw new IllegalArgumentException("Unexpected shuffle block id format: "+blockIds[i]);
                }
                if (Integer.parseInt(blockIdParts[1]) != shuffleId){
                    throw new IllegalArgumentException("Expected shuffleId="+shuffleId+", got:"+blockIds[i]);
                }
                mapIdAndReduceIds[2*i]=Integer.parseInt(blockIdParts[2]);
                mapIdAndReduceIds[2*i+1]=Integer.parseInt(blockIdParts[3]);
            }
            return mapIdAndReduceIds;
        }
        private int[] shuffleReduceIdAndChunkIds(String [] blockIds,int shuffleId,int shuffleMergeId){
            final int[] reduceIdAndChunkIds = new int[2*blockIds.length];
            for (int i = 0; i < blockIds.length; i++) {
                String[] blockIdParts = blockIds[i].split("_");
                if (blockIdParts.length != 5 || !blockIdParts[0].equals(SHUFFLE_CHUNK_ID)) {
                    throw new IllegalArgumentException("Unexpected shuffle chunk id format: "+blockIds[i]);
                }
                if (Integer.parseInt(blockIdParts[1])!=shuffleId ||
                Integer.parseInt(blockIdParts[2])!=shuffleMergeId){
                    throw new IllegalArgumentException(String.format("Expected shuffleId = %s and shuffleMergeId = %s but got %s",shuffleId,shuffleMergeId,blockIds[i]));
                }
                reduceIdAndChunkIds[2*i]=Integer.parseInt(blockIdParts[3]);
                reduceIdAndChunkIds[2*i+1]=Integer.parseInt(blockIdParts[4]);
            }
            return reduceIdAndChunkIds;
        }

        @Override
        public boolean hasNext() {
            return index<size;
        }

        @Override
        public ManagedBuffer next() {
            final ManagedBuffer block = blockDataForIndexFn.apply(index);
            index+=2;
            metrics.blockTransferRate.mark();
            metrics.blockTransferMessageRate.mark();
            metrics.blockTransferRateBytes.mark(block!=null ? block.size():0);
            return block;
        }
    }
}
