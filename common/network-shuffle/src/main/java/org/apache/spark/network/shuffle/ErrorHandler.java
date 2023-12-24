package org.apache.spark.network.shuffle;

import com.google.common.base.Throwables;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.network.server.BlockPushNonFatalFailure;

import java.io.FileNotFoundException;
import java.net.ConnectException;

@Evolving
public interface ErrorHandler {
    boolean shouldRetryError(Throwable t);
    default boolean shouldLogError(Throwable t){
        return true;
    }
    ErrorHandler NOOP_ERROR_HANDLER=t->true;

    class BlockPushErrorHandler implements ErrorHandler{
        public static final String IOEXCEPTIONS_EXCEEDED_THRESHOLD_PREFIX=
                "IOExceptions exceeded the threshold";
        public static final String STALE_SHUFFLE_FINALIZE_SUFFIX=
                "stale shuffle finalize request as shuffle blocks of a higher shuffleMergeId for the" +
                        " shuffle is already being pushed";

        @Override
        public boolean shouldRetryError(Throwable t) {
            if (t.getCause() instanceof ConnectException ||
            t.getCause() instanceof FileNotFoundException){
                return false;
            }
            return !(t instanceof BlockPushNonFatalFailure && BlockPushNonFatalFailure
                    .shouldNotRetryErrorCode(((BlockPushNonFatalFailure) t).getReturnCode()));
        }

        @Override
        public boolean shouldLogError(Throwable t) {
            return !(t instanceof BlockPushNonFatalFailure);
        }
    }

    class BlockFetchErrorHandler implements ErrorHandler {
        public static final String STALE_SHUFFLE_BLOCK_FETCH =
                "stale shuffle block fetch request as shuffle blocks of a higher shuffleMergeId for the"
                        + " shuffle is available";

        @Override
        public boolean shouldRetryError(Throwable t) {
            return !Throwables.getStackTraceAsString(t).contains(STALE_SHUFFLE_BLOCK_FETCH);
        }

        @Override
        public boolean shouldLogError(Throwable t) {
            return !Throwables.getStackTraceAsString(t).contains(STALE_SHUFFLE_BLOCK_FETCH);
        }
    }
}
