package org.apache.spark.network.shuffle;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.protocol.Encoders;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MergedBlockMeta {
    private final int numChunks;
    private final ManagedBuffer chunksBitmapBuffer;

    public MergedBlockMeta(int numChunks, ManagedBuffer chunksBitmapBuffer) {
        this.numChunks = numChunks;
        this.chunksBitmapBuffer = Preconditions.checkNotNull(chunksBitmapBuffer);
    }

    public int getNumChunks() {
        return numChunks;
    }

    public ManagedBuffer getChunksBitmapBuffer() {
        return chunksBitmapBuffer;
    }

    public RoaringBitmap[] readChunkBitmaps() throws IOException {
        ByteBuf buf = Unpooled.wrappedBuffer(chunksBitmapBuffer.nioByteBuffer());
        List<RoaringBitmap> bitmaps = new ArrayList<>();
        while (buf.isReadable()) {
            bitmaps.add(Encoders.Bitmaps.decode(buf));
        }
        assert (bitmaps.size() == numChunks);
        return bitmaps.toArray(new RoaringBitmap[0]);
    }
}
