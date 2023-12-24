package org.apache.spark.network.shuffle.checksum;

import com.google.common.io.ByteStreams;
import org.apache.spark.annotation.Private;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

@Private
public class ShuffleChecksumHelper {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleChecksumHelper.class);
    public static final int CHECKSUM_CALCULATION_BUFFER = 8192;
    public static final Checksum[] EMPTY_CHECKSUM = new Checksum[0];
    public static final long[] EMPTY_CHECKSUM_VALUE = new long[0];

    public static Checksum[] createPartitionChecksums(int numPartitions, String algorithm) {
        return getChecksumByAlgorithm(numPartitions,algorithm);
    }

    private static Checksum[] getChecksumByAlgorithm(int num, String algorithm) {
        Checksum[] checksums;
        switch (algorithm) {
            case "ADLER32":
                checksums=new Adler32[num];
                for (int i = 0; i < num; i++) {
                    checksums[i] = new Adler32();
                }
                return checksums;
            case "CRC32":
                checksums=new CRC32[num];
                for (int i = 0; i < num; i++) {
                    checksums[i]=new CRC32();
                }
                return checksums;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported shuffle checksum algorithm: "+algorithm
                );
        }
    }

    public static Checksum getChecksumByAlgorithm(String algorithm) {
        return getChecksumByAlgorithm(1,algorithm)[0];
    }


    public static String getChecksumFileName(String blockName, String algorithm) {
        return String.format("%s.%s",blockName,algorithm);
    }

    private static long readChecksumByReduceId(File checksumFile, int reduceId)throws IOException {
        try (DataInputStream in = new DataInputStream(new FileInputStream(checksumFile))) {
            ByteStreams.skipFully(in,reduceId*8L);
            return in.readLong();
        }
    }

    private static long calculateChecksumForPartition(ManagedBuffer partitionData,Checksum checksumAlgo)throws IOException {
        InputStream in = partitionData.createInputStream();
        byte[] buffer = new byte[CHECKSUM_CALCULATION_BUFFER];
        try (CheckedInputStream checksumIn = new CheckedInputStream(in,checksumAlgo)) {
            while (checksumIn.read(buffer,0,CHECKSUM_CALCULATION_BUFFER)!=-1){}
            return checksumAlgo.getValue();
        }
    }

    public static Cause diagnoseCorruption(
            String algorithm,
            File checksumFile,
            int reduceId,
            ManagedBuffer partitionData,
            long checksumByReader
    ) {
        Cause cause;
        long duration=-1L;
        long checksumByWriter=-1L;
        long checksumByReCalculation=-1L;
        try {
            long diagnoseStartNs = System.nanoTime();
            Checksum checksumAlgo = getChecksumByAlgorithm(algorithm);
            checksumByWriter=readChecksumByReduceId(checksumFile,reduceId);
            checksumByReCalculation=calculateChecksumForPartition(partitionData,checksumAlgo);
            duration= TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-diagnoseStartNs);
            if (checksumByWriter!=checksumByReCalculation){
                cause=Cause.DISK_ISSUE;
            }else if (checksumByWriter!=checksumByReader){
                cause=Cause.NETWORK_ISSUE;
            }else {
                cause=Cause.CHECKSUM_VERIFY_PASS;
            }
        }catch (UnsupportedOperationException e){
            cause=Cause.UNSUPPORTED_CHECKSUM_ALGORITHM;
        }catch (FileNotFoundException e){
            logger.warn("Checksum file "+checksumFile.getName()+" doesn't exist");
            cause=Cause.UNKNOWN_ISSUE;
        }catch (Exception e){
            logger.warn("Unable to diagnose shuffle block corruption",e);
            cause=Cause.UNKNOWN_ISSUE;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Shuffle corruption diagnosis took {} ms, checksum file {}, cause {}, " +
                    "checksumByReader {}, checksumByWriter {}, checksumByReCalculation {}",
                    duration,checksumFile.getAbsolutePath(),cause,
                    checksumByReader,checksumByWriter,checksumByReCalculation);
        }else {
            logger.info("Shuffle corruption diagnosis took {} ms, checksum file {}, cause {}",
                    duration, checksumFile.getAbsolutePath(), cause);
        }
        return cause;
    }
}
