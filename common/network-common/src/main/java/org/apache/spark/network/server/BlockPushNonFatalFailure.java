package org.apache.spark.network.server;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

public class BlockPushNonFatalFailure extends RuntimeException{
    public static final String TOO_LATE_BLOCK_PUSH_MESSAGE_SUFFIX=
            " is received after merged shuffle is finalized";
    public static final String TOO_OLD_ATTEMPT_SUFFIX=
            " is from an older app attempt";
    public static final String STALE_BLOCK_PUSH_MESSAGE_SUFFIX=
            " is a stale block push from an indeterminate stage retry";
    public static final String BLOCK_APPEND_COLLISION_MSG_SUFFIX=
            " experienced merge collision on the server side";
    private ByteBuffer response;
    private ReturnCode returnCode;
    public BlockPushNonFatalFailure(ByteBuffer response,String msg){
        super(msg);
        this.response=response;
    }
    public BlockPushNonFatalFailure(ReturnCode returnCode, String msg){
        super(msg);
        this.returnCode=returnCode;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    public ByteBuffer getResponse() {
        Preconditions.checkNotNull(response);
        return response;
    }


    public enum ReturnCode{
        SUCCESS(0,"")
        ,TOO_LATE_BLOCK_PUSH(1,TOO_LATE_BLOCK_PUSH_MESSAGE_SUFFIX)
        ,BLOCK_APPEND_COLLISION_DETECTED(2,BLOCK_APPEND_COLLISION_MSG_SUFFIX)
        ,STALE_BLOCK_PUSH(3,STALE_BLOCK_PUSH_MESSAGE_SUFFIX)
        ,TOO_OLD_ATTEMPT_PUSH(4,TOO_OLD_ATTEMPT_SUFFIX);

        private final byte id;
        private final String errorMsgSuffix;

        ReturnCode(int id, String errorMsgSuffix) {
            assert id<128 : "Cannot have more than 128 block push return code";
            this.id = (byte) id;
            this.errorMsgSuffix = errorMsgSuffix;
        }

        public byte id() {
            return id;
        }
    }

    public ReturnCode getReturnCode() {
        // Ensure we do not invoke this method if returnCode is not set
        Preconditions.checkNotNull(returnCode);
        return returnCode;
    }

    public static ReturnCode getReturnCode(byte id){
        switch (id){
            case 0: return ReturnCode.SUCCESS;
            case 1: return ReturnCode.TOO_LATE_BLOCK_PUSH;
            case 2: return ReturnCode.BLOCK_APPEND_COLLISION_DETECTED;
            case 3: return ReturnCode.STALE_BLOCK_PUSH;
            case 4: return ReturnCode.TOO_OLD_ATTEMPT_PUSH;
            default: throw new IllegalArgumentException("Unknown block push return code: "+id);
        }
    }

    public static boolean shouldNotRetryErrorCode(ReturnCode returnCode){
        return returnCode==ReturnCode.TOO_LATE_BLOCK_PUSH ||
                returnCode==ReturnCode.STALE_BLOCK_PUSH ||
                returnCode==ReturnCode.TOO_OLD_ATTEMPT_PUSH;
    }

    public static String getErrorMsg(String blockId, ReturnCode errorCode){
        Preconditions.checkArgument(errorCode!=ReturnCode.SUCCESS);
        return "Block "+blockId+errorCode.errorMsgSuffix;
    }
}
