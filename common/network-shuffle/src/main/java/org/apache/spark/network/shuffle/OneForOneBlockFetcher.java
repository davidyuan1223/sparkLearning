package org.apache.spark.network.shuffle;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class OneForOneBlockFetcher {
    private static final Logger logger = LoggerFactory.getLogger(OneForOneBlockFetcher.class);
    private static final String SHUFFLE_BLOCK_PREFIX="shuffle_";
    private static final String SHUFFLE_CHUNK_PREFIX="shuffleChunk_";
    private static final String SHUFFLE_BLOCK_SPLIT="shuffle";
    private static final String SHUFFLE_CHUNK_SPLIT="shuffleChunk";
    private final TransportClient client;
    private final BlockTransferMessage message;
    private final String[] blockIds;
    private final BlockFetchingListener listener;
    private final ChunkReceivedCallback chunkCallback;
    private final TransportConf transportConf;
    private final DownloadFileManager downloadFileManager;
    private StreamHandle streamHandle=null;

    public OneForOneBlockFetcher(TransportClient client,
                                 String appId,
                                 String execId,
                                 String[] blockIds,
                                 BlockFetchingListener listener,
                                 TransportConf transportConf){
        this(client,appId,execId,blockIds,listener,transportConf,null);
    }

    public OneForOneBlockFetcher(TransportClient client,String appId,String execId,
                                 String[] blockIds,BlockFetchingListener listener,TransportConf transportConf,
                                 DownloadFileManager downloadFileManager){
        this.client=client;
        this.listener=listener;
        this.chunkCallback=new ChunkCallback();
        this.transportConf=transportConf;
        this.downloadFileManager=downloadFileManager;
        if (blockIds.length==0) {
            throw new IllegalArgumentException("Zero-sized blockIds array");
        }
        if (!transportConf.useOldFetchProtocol() && areShuffleBlocksOrChunks(blockIds)) {
            this.blockIds=new String[blockIds.length];
            this.message=createFetchShuffleBlocksOrChunksMsg(appId,execId,blockIds);
        }else {
            this.blockIds=blockIds;
            this.message=new OpenBlocks(appId,execId,blockIds);
        }
    }

    private boolean areShuffleBlocksOrChunks(String[] blockIds){
        if (isAnyBlockNotStartWithShuffleBlockPrefix(blockIds)){
            return isAllBlocksStartWithShuffleChunkPrefix(blockIds);
        }
        return true;
    }

    private static boolean isAnyBlockNotStartWithShuffleBlockPrefix(String[] blockIds){
        for (String blockId : blockIds) {
            if (!blockId.startsWith(SHUFFLE_BLOCK_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isAllBlocksStartWithShuffleChunkPrefix(String[] blockIds){
        for (String blockId : blockIds) {
            if (!blockId.startsWith(SHUFFLE_CHUNK_PREFIX)) {
                return false;
            }
        }
        return true;
    }

    private AbstractFetchShuffleBlocks createFetchShuffleBlocksOrChunksMsg(String appId,String execId,String[] blockIds){
        if (blockIds[0].startsWith(SHUFFLE_CHUNK_PREFIX)){
            return createFetchShuffleChunksMsg(appId,execId,blockIds);
        }else {
            return createFetchShuffleBlocksMsg(appId,execId,blockIds);
        }
    }

    private AbstractFetchShuffleBlocks createFetchShuffleBlocksMsg(String appId,String execId,String[] blockIds){
        String[] firstBlock = splitBlockId(blockIds[0]);
        int shuffleId = Integer.parseInt(firstBlock[1]);
        boolean batchFetchEnabled = firstBlock.length==5;
        Map<Long,BlocksInfo> mapIdToBlocksInfo = new LinkedHashMap<>();
        for (String blockId : blockIds) {
            String[] blockIdParts = splitBlockId(blockId);
            if (Integer.parseInt(blockIdParts[1])!=shuffleId){
                throw new IllegalArgumentException("Expected shuffleId="+shuffleId+", got:"+blockId);
            }
            long mapId = Long.parseLong(blockIdParts[2]);
            BlocksInfo blocksInfoByMapId = mapIdToBlocksInfo.computeIfAbsent(mapId, id -> new BlocksInfo());
            blocksInfoByMapId.blockIds.add(blockId);
            blocksInfoByMapId.ids.add(Integer.parseInt(blockIdParts[3]));
            if (batchFetchEnabled){
                assert (blockIdParts.length==5);
                blocksInfoByMapId.ids.add(Integer.parseInt(blockIdParts[4]));
            }
        }
        int[][] reduceIdsArray = getSecondaryIds(mapIdToBlocksInfo);
        long[] mapIds = Longs.toArray(mapIdToBlocksInfo.keySet());
        return new FetchShuffleBlocks(appId,execId,shuffleId,mapIds,reduceIdsArray,batchFetchEnabled);
    }

    private AbstractFetchShuffleBlocks createFetchShuffleChunksMsg(String appId,String execId,String[] blockIds){
        String[] firstBlock = splitBlockId(blockIds[0]);
        int shuffleId = Integer.parseInt(firstBlock[1]);
        int shuffleMergeId = Integer.parseInt(firstBlock[2]);
        Map<Integer,BlocksInfo> reduceIdToBlocksInfo = new LinkedHashMap<>();
        for (String blockId : blockIds) {
            String[] blockIdParts = splitBlockId(blockId);
            if (Integer.parseInt(blockIdParts[1])!=shuffleId ||
            Integer.parseInt(blockIdParts[2])!=shuffleMergeId){
                throw new IllegalArgumentException(String.format("Expected shuffleId = %s and"
                        + " shuffleMergeId = %s but got %s", shuffleId, shuffleMergeId, blockId));
            }
            int reduceId = Integer.parseInt(blockIdParts[3]);
            BlocksInfo blocksInfoByReduceId = reduceIdToBlocksInfo.computeIfAbsent(reduceId, id -> new BlocksInfo());
            blocksInfoByReduceId.blockIds.add(blockId);
            blocksInfoByReduceId.ids.add(Integer.parseInt(blockIdParts[4]));
        }
        int[][] chunkIdsArray=getSecondaryIds(reduceIdToBlocksInfo);
        int[] reduceIds= Ints.toArray(reduceIdToBlocksInfo.keySet());
        return new FetchShuffleBlockChunks(appId, execId, shuffleId, shuffleMergeId, reduceIds,
                chunkIdsArray);
    }

    private int[][] getSecondaryIds(Map<? extends Number,BlocksInfo> primaryIdsToBlockInfo){
        int[][] secondaryIds = new int[primaryIdsToBlockInfo.size()][];
        int blockIdIndex=0;
        int secIndex=0;
        for (BlocksInfo blocksInfo : primaryIdsToBlockInfo.values()) {
            secondaryIds[secIndex++]=Ints.toArray(blocksInfo.ids);
            for (String blockId : blocksInfo.blockIds) {
                this.blockIds[blockIdIndex++]=blockId;
            }
        }
        assert blockIdIndex==this.blockIds.length;
        return secondaryIds;
    }

    private String[] splitBlockId(String blockId) {
        String[] blockIdParts = blockId.split("_");
        if (blockIdParts.length < 4 || blockIdParts.length>5){
            throw new IllegalArgumentException("Unexpected shuffle block id format: "+blockId);
        }
        if (blockIdParts.length == 4 && !blockIdParts[0].equals(SHUFFLE_BLOCK_SPLIT)){
            throw new IllegalArgumentException("Unexpected shuffle block id format: "+blockId);
        }
        if (blockIdParts.length==5 &&
        !(blockIdParts[0].equals(SHUFFLE_BLOCK_SPLIT) ||
                blockIdParts[0].equals(SHUFFLE_CHUNK_SPLIT))){
            throw new IllegalArgumentException("Unexpected shuffle block id format: "+blockId);
        }
        return blockIdParts;
    }

    private class ChunkCallback implements ChunkReceivedCallback {
        @Override
        public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
            listener.onBlockFetchSuccess(blockIds[chunkIndex],buffer);
        }

        @Override
        public void onFailure(int chunkIndex, Throwable e) {
            String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
            failRemainingBlocks(remainingBlockIds,e);
        }
    }
    private static class BlocksInfo {
        final ArrayList<Integer> ids;
        final ArrayList<String > blockIds;
        BlocksInfo(){
            this.ids=new ArrayList<>();
            this.blockIds=new ArrayList<>();
        }
    }


    public void start(){
        client.sendRpc(message.toByteBuffer(), new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                try{
                    streamHandle=(StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
                    logger.trace("Successfully opend blocks {}, preparing to fetch chunks.",streamHandle);
                    for (int i = 0; i < streamHandle.numChunks; i++) {
                        if (downloadFileManager != null) {
                            client.stream(OneForOneStreamManager.getStreamChunkId(streamHandle.streamId,i),
                                    new DownloadCallback(i));
                        }else {
                            client.fetchChunk(streamHandle.streamId, i,chunkCallback);
                        }
                    }
                }catch (Exception e){
                    logger.error("Failed while starting block fetches after success",e);
                    failRemainingBlocks(blockIds,e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("Failed while starting block fetches", e);
                failRemainingBlocks(blockIds, e);
            }
        });
    }

    private void failRemainingBlocks(String[] failedBlockIds,Throwable e){
        for (String failedBlockId : failedBlockIds) {
            try {
                listener.onBlockFetchFailure(failedBlockId,e);
            }catch (Exception e2){
                logger.error("Error in block fetch failure callback", e2);
            }
        }
    }

    private class DownloadCallback implements StreamCallback{
        private DownloadFileWritableChannel channel =null;
        private DownloadFile targetFile = null;
        private int chunkIndex;
        DownloadCallback(int chunkIndex) throws IOException {
            this.targetFile = downloadFileManager.createTempFile(transportConf);
            this.channel = targetFile.openForWriting();
            this.chunkIndex = chunkIndex;
        }

        @Override
        public void onData(String streamId, ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
        }

        @Override
        public void onComplete(String streamId) throws IOException {
            listener.onBlockFetchSuccess(blockIds[chunkIndex],channel.closeAndRead());
            if (!downloadFileManager.registerTempFileToClean(targetFile)) {
                targetFile.delete();
            }
        }

        @Override
        public void onFailure(String streamId, Throwable cause) throws IOException {
            channel.close();
            String[] remainingBlockIds = Arrays.copyOfRange(blockIds,chunkIndex,blockIds.length);
            failRemainingBlocks(remainingBlockIds,cause);
            targetFile.delete();
        }
    }
}
