package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.shuffle.protocol.mesos.RegisterDriver;
import org.apache.spark.network.shuffle.protocol.mesos.ShuffleServiceHeartbeat;

import java.nio.ByteBuffer;

public abstract class BlockTransferMessage implements Encodable {
    public enum Type {
        OPEN_BLOCKS(0),UPLOAD_BLOCK(1),REGISTER_EXECUTOR(2),STREAM_HANDLE(3),
        REGISTER_DRIVER(4),HEARTBEAT(5),UPLOAD_BLOCK_STREAM(6),REMOVE_BLOCKS(7),
        BLOCKS_REMOVED(8),FETCH_SHUFFLE_BLOCKS(9),GET_LOCAL_DIRS_FOR_EXECUTORS(10),
        LOCAL_DIRS_FOR_EXECUTORS(11),PUSH_BLOCK_STREAM(12),FINALIZE_SHUFFLE_MERGE(13),
        MERGE_STATUSES(14),FETCH_SHUFFLE_BLOCK_CHUNKS(15),DIAGNOSE_CORRUPTION(16),
        CORRUPTION_CAUSE(17),PUSH_BLOCK_RETURN_CODE(18),REMOVE_SHUFFLE_MERGE(19);

        private final byte id;

        Type(int id) {
            assert id<128 : "Cannot have more than 128 message types";
            this.id = (byte) id;
        }

        public byte id() {
            return id;
        }
    }
    protected abstract Type type();

    public static class Decoder {

        public static BlockTransferMessage fromByteBuffer(ByteBuffer msg) {
            ByteBuf buf = Unpooled.wrappedBuffer(msg);
            byte type = buf.readByte();
            switch (type) {
                case 0: return OpenBlocks.decode(buf);
                case 1: return UploadBlock.decode(buf);
                case 2: return RegisterExecutor.decode(buf);
                case 3: return StreamHandle.decode(buf);
                case 4: return RegisterDriver.decode(buf);
                case 5: return ShuffleServiceHeartbeat.decode(buf);
                case 6: return UploadBlockStream.decode(buf);
                case 7: return RemoveBlocks.decode(buf);
                case 8: return BlocksRemoved.decode(buf);
                case 9: return FetchShuffleBlocks.decode(buf);
                case 10: return GetLocalDirsForExecutors.decode(buf);
                case 11: return LocalDirsForExecutors.decode(buf);
                case 12: return PushBlockStream.decode(buf);
                case 13: return FinalizeShuffleMerge.decode(buf);
                case 14: return MergeStatuses.decode(buf);
                case 15: return FetchShuffleBlockChunks.decode(buf);
                case 16: return DiagnoseCorruption.decode(buf);
                case 17: return CorruptionCause.decode(buf);
                case 18: return BlockPushReturnCode.decode(buf);
                case 19: return RemoveShuffleMerge.decode(buf);
                default: throw new IllegalArgumentException("Unknown message type: " + type);
            }
        }
    }

    public ByteBuffer toByteBuffer(){
        ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
        buf.writeByte(type().id);
        encode(buf);
        assert buf.writableBytes()==0: "Writable bytes remain: "+buf.writableBytes();
        return buf.nioBuffer();
    }
}
