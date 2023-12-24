package org.apache.spark.network.shuffle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.network.server.BlockPushNonFatalFailure;
import org.apache.spark.network.shuffle.protocol.BlockPushReturnCode;
import org.apache.spark.network.shuffledb.StoreVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

public class RemoteBlockPushResolver implements MergedShuffleFileManager{
    private static final Logger logger = LoggerFactory.getLogger(RemoteBlockPushResolver.class);
    public static final String MERGED_SHUFFLE_FILE_NAME_PREFIX="shuffleMerged";
    public static final String SHUFFLE_META_DELIMITER=":";
    public static final String ATTEMPT_ID_KEY="attemptId";
    private static final int UNDEFINED_ATTEMPT_ID=-1;
    public static final int DELETE_ALL_MERGED_SHUFFLE=-1;
    private static final String DB_KEY_DELIMITER=";";
    private static final ErrorHandler.BlockPushErrorHandler ERROR_HANDLER=createErrorHandler();
    private static final ByteBuffer SUCCESS_RESPONSE=
            new BlockPushReturnCode(BlockPushNonFatalFailure.ReturnCode.SUCCESS.id(), "").toByteBuffer()
            .asReadOnlyBuffer();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String APP_ATTEMPT_SHUFFLE_FINALIZE_STATUS_KEY_PREFIX=
            "AppAttemptShuffleFinalized";
    private static final String APP_ATTEMPT_PATH_KEY_PREFIX=
            "AppAttemptPathInfo";
    private static final StoreVersion CURRENT_VERSION=
            new StoreVersion(1,0);
    @VisibleForTesting
    final ConcurrentMap<String ,AppShuffleInfo> appsShuffleInfo;





    public static class AppShuffleInfo{
        @VisibleForTesting
        final String appId;
        @VisibleForTesting
        final int attemptId;
        private final AppPathsInfo appPathsInfo;
        private final ConcurrentMap<Integer,AppShuffleMergePartitionsInfo> shuffles;
    }

    @VisibleForTesting
    public static class AppPathsInfo{
        @JsonFormat(shape = JsonFormat.Shape.ARRAY)
        @JsonProperty("activeLocalDirs")
        private final String[] activeLocalDirs;
        @JsonProperty("subDirsPerLocalDir")
        private final int subDirsPerLocalDir;

        @JsonCreator
        public AppPathsInfo(@JsonFormat(shape = JsonFormat.Shape.ARRAY)
                            @JsonProperty("activeLocalDirs") String[] activeLocalDirs,
                            @JsonProperty("subDirsPerLocalDir") int subDirsPerLocalDir){
            this.activeLocalDirs=activeLocalDirs;
            this.subDirsPerLocalDir=subDirsPerLocalDir;
        }

        private AppPathsInfo(String appId,String [] localDirs,String mergeDirectory,int subDirsPerLocalDir){
            activeLocalDirs= Arrays.stream(localDirs)
                    .map(localDir ->
                            Paths.get(localDir).getParent().resolve(mergeDirectory).toFile().getPath())
                    .toArray(String[]::new);
            this.subDirsPerLocalDir=subDirsPerLocalDir;
            if (logger.isInfoEnabled()) {
                logger.info("Updated active local dirs {} and sub dirs {} for application {}",
                        Arrays.toString(activeLocalDirs),subDirsPerLocalDir, appId);
            }
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AppPathsInfo appPathsInfo = (AppPathsInfo) o;
            return subDirsPerLocalDir == appPathsInfo.subDirsPerLocalDir &&
                    Arrays.equals(activeLocalDirs, appPathsInfo.activeLocalDirs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subDirsPerLocalDir) * 41 + Arrays.hashCode(activeLocalDirs);
        }
    }

    public static class AppShuffleMergePartitionsInfo{
        private static final Map<Integer,AppShufflePartitionInfo> SHUFFLE_FINALIZED_MARKER
                = Collections.emptyMap();
    }

    public static class AppShufflePartitionInfo{
        private final AppAttemptShuffleMergeId appAttemptShuffleMergeId;
    }
}
