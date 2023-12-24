package org.apache.spark.network.shuffle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.sasl.SaslTimeoutException;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RetryingBlockTransferor {
    public interface BlockTransferStarter {
        void createAndStart(String[] blockIds, BlockTransferListener listener)throws IOException,InterruptedException;
    }
    private class RetryingBlockTransferListener implements BlockFetchingListener,BlockPushingListener{
        private void handleBlockTransferSuccess(String blockId, ManagedBuffer data){
            boolean shouldForwardSuccess = false;
            synchronized (RetryingBlockTransferor.this) {
                if (this==currentListener && outstandingBlocksIds.contains(blockId)) {
                    outstandingBlocksIds.remove(blockId);
                    shouldForwardSuccess=true;
                    if (saslRetryCount>0){
                        Preconditions.checkState(retryCount>=saslRetryCount,"retryCount must be greater than or equal to saslRetryCount");
                        retryCount-=saslRetryCount;
                        saslRetryCount=0;
                    }
                }
            }
            if (shouldForwardSuccess) {
                listener.onBlockTransferSuccess(blockId, data);
            }
        }

        private void handleBlockTransferFailure(String blockId,Throwable exception){
            boolean shouldForwardFailure = false;
            synchronized (RetryingBlockTransferor.this) {
                if (this==currentListener && outstandingBlocksIds.contains(blockId)){
                    if (shouldRetry(exception)){
                        initiateRetry(exception);
                    }else {
                        if (errorHandler.shouldLogError(exception)) {
                            logger.error(
                                    String.format("Failed to %s block %s, and will not retry (%s retries)",
                                            listener.getTransferType(),blockId,retryCount),exception
                            );
                        }else {
                            logger.debug(
                                    String.format("Failed to %s block %s, and will not retry (%s retries)",
                                            listener.getTransferType(),blockId,retryCount),exception
                            );
                        }
                        outstandingBlocksIds.remove(blockId);
                        shouldForwardFailure=true;
                    }
                }
            }
            if (shouldForwardFailure) {
                listener.onBlockTransferFailure(blockId, exception);
            }
        }

        @Override
        public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
            handleBlockTransferSuccess(blockId, data);
        }

        @Override
        public void onBlockFetchFailure(String blockId, Throwable exception) {
            handleBlockTransferFailure(blockId, exception);
        }

        @Override
        public void onBlockPushSuccess(String blockId, ManagedBuffer data) {
            handleBlockTransferSuccess(blockId, data);
        }

        @Override
        public void onBlockPushFailure(String blockId, Throwable exception) {
            handleBlockTransferFailure(blockId, exception);
        }

        @Override
        public void onBlockTransferSuccess(String blockId, ManagedBuffer data) {
            throw new RuntimeException(
                    "Invocation on RetryingBlockTransferListener.onBlockTransferSuccess is unexpected.");
        }

        @Override
        public void onBlockTransferFailure(String blockId, Throwable exception) {
            throw new RuntimeException(
                    "Invocation on RetryingBlockTransferListener.onBlockTransferSuccess is unexpected.");
        }

        @Override
        public String getTransferType() {
            throw new RuntimeException(
                    "Invocation on RetryingBlockTransferListener.getTransferType is unexpected.");
        }
    }
    private static final ExecutorService executorService = Executors.newCachedThreadPool(
            NettyUtils.createThreadFactory("Block Transfer Retry")
    );
    private static final Logger logger = LoggerFactory.getLogger(RetryingBlockTransferor.class);
    private final BlockTransferStarter transferStarter;
    private final BlockTransferListener listener;
    private final int maxRetris;
    private final int retryWaitTime;
    private int retryCount=0;
    private int saslRetryCount=0;
    private final LinkedHashSet<String> outstandingBlocksIds;
    private RetryingBlockTransferListener currentListener;
    private final boolean enableSaslRetries;
    private final ErrorHandler errorHandler;
    public RetryingBlockTransferor(TransportConf conf,BlockTransferStarter transferStarter,
                                   String[] blockIds,BlockTransferListener listener,ErrorHandler errorHandler){
        this.transferStarter=transferStarter;
        this.listener=listener;
        this.maxRetris=conf.maxIORetries();
        this.retryWaitTime=conf.ioRetryWaitTimeMs();
        this.outstandingBlocksIds= Sets.newLinkedHashSet();
        Collections.addAll(outstandingBlocksIds,blockIds);
        this.currentListener=new RetryingBlockTransferListener();
        this.errorHandler=errorHandler;
        this.enableSaslRetries=conf.enableSaslRetries();
        this.saslRetryCount=0;
    }
    public RetryingBlockTransferor(
            TransportConf conf,
            BlockTransferStarter transferStarter,
            String[] blockIds,
            BlockFetchingListener listener) {
        this(conf, transferStarter, blockIds, listener, ErrorHandler.NOOP_ERROR_HANDLER);
    }
    public void start(){
        transferAllOutstanding();
    }
    private void transferAllOutstanding(){
        String[] blockIdsToTransfer;
        int numRetries;
        RetryingBlockTransferListener myListener;
        synchronized (this) {
            blockIdsToTransfer=outstandingBlocksIds.toArray(new String[0]);
            numRetries=retryCount;
            myListener=currentListener;
        }
        try {
            transferStarter.createAndStart(blockIdsToTransfer,myListener);
        }catch (Exception e){
            logger.error(String.format("Exception while beginning %s of %s outstanding blocks %s",
                    listener.getTransferType(),blockIdsToTransfer.length,
                    numRetries>0 ? "(after "+numRetries+" retries)":""),e);
            if (shouldRetry(e)){
                initiateRetry(e);
            }else {
                for (String bid : blockIdsToTransfer) {
                    listener.onBlockTransferFailure(bid,e);
                }
            }
        }
    }
    private synchronized void initiateRetry(Throwable e){
        if (enableSaslRetries && e instanceof SaslTimeoutException){
            saslRetryCount+=1;
        }
        retryCount+=1;
        currentListener=new RetryingBlockTransferListener();
        logger.info("Retrying {} ({}/{}) for {} outstanding blocks after {} ms",
                listener.getTransferType(),retryCount,maxRetris,outstandingBlocksIds.size(),
                retryWaitTime);
        executorService.submit(() -> {
            Uninterruptibles.sleepUninterruptibly(retryWaitTime, TimeUnit.MILLISECONDS);
            transferAllOutstanding();
        });
    }

    private synchronized boolean shouldRetry(Throwable e){
        boolean isIOException = e instanceof IOException
                || e.getCause() instanceof IOException;
        boolean isSaslTimeout = enableSaslRetries && e instanceof SaslTimeoutException;
        if (!isSaslTimeout && saslRetryCount>0) {
            Preconditions.checkState(retryCount >= saslRetryCount,
                    "retryCount must be greater than or equal to saslRetryCount");
            retryCount-=saslRetryCount;
            saslRetryCount=0;
        }
        boolean hasRemainingRetries = retryCount<maxRetris;
        boolean shouldRetry = (isSaslTimeout || isIOException)
                && hasRemainingRetries && errorHandler.shouldRetryError(e);
        return shouldRetry;
    }


    @VisibleForTesting
    public int getRetryCount() {
        return retryCount;
    }

}
