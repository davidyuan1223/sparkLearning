package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.buffer.ManagedBuffer;

public interface Message extends Encodable{
    Type type();
    ManagedBuffer body();
    boolean isBodyInFrame();



    enum Type implements Encodable{
        ChunkFetchRequest(0),
        ChunkFetchSuccess(1),
        ChunkFetchFailure(2),
        RpcRequest(3),
        RpcResponse(4),
        RpcFailure(5),
        StreamRequest(6),
        StreamResponse(7),
        StreamFailure(8),
        OneWayMessage(9),
        UploadStream(10),
        MergedBlockMetaRequest(11),
        MergedBlockMetaSuccess(12),
        User(-1);

        private final byte id;
        Type(int id){
            assert id < 128 : "Cannot have more than 128 message types";
            this.id=(byte) id;
        }
        public byte id(){
            return id;
        }

        @Override
        public int encodedLength() {
            return 1;
        }

        @Override
        public void encode(ByteBuf buf) {
            buf.writeByte(id);
        }

        public static Type decode(ByteBuf buf) {
            byte id = buf.readByte();
            switch (id) {
                case 0: return ChunkFetchRequest;
                case 1: return ChunkFetchSuccess;
                case 2: return ChunkFetchFailure;
                case 3: return RpcRequest;
                case 4: return RpcResponse;
                case 5: return RpcFailure;
                case 6: return StreamRequest;
                case 7: return StreamResponse;
                case 8: return StreamFailure;
                case 9: return UploadStream;
                case 10: return OneWayMessage;
                case 11: return MergedBlockMetaRequest;
                case 12: return MergedBlockMetaSuccess;
                case -1: throw new IllegalArgumentException("User type message cannot be decoded.");
                default: throw new IllegalArgumentException("Unknown message type: "+id);
            }
        }
    }
}
