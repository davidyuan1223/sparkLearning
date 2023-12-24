package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Encoders {
    public static class Strings {
        public static int encodedLength(String s){
            return 4+s.getBytes(StandardCharsets.UTF_8).length;
        }
        public static void encode(ByteBuf buf, String s){
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            buf.writeInt(bytes.length);
            buf.writeBytes(bytes);
        }
        public static String decode(ByteBuf buf){
            int length = buf.readInt();
            byte[] bytes = new byte[length];
            buf.readBytes(bytes);
            return new String(bytes,StandardCharsets.UTF_8);
        }
    }
    public static class Bitmaps{
        public static int encodedLength(RoaringBitmap b){
            b.trim();
            b.runOptimize();
            return b.serializedSizeInBytes();
        }

        public static void encode(ByteBuf buf, RoaringBitmap b){
            ByteBuffer byteBuffer = buf.nioBuffer(buf.writerIndex(), buf.writableBytes());
            b.serialize(byteBuffer);
            buf.writerIndex(buf.writerIndex()+byteBuffer.position());
        }

        public static RoaringBitmap decode(ByteBuf buf){
            RoaringBitmap bitmap = new RoaringBitmap();
            try {
                bitmap.deserialize(buf.nioBuffer());
                buf.readerIndex(buf.readerIndex()+bitmap.serializedSizeInBytes());
            }catch (IOException e){
                throw new RuntimeException("Exception while decoding bitmap", e);
            }
            return bitmap;
        }
    }
    public static class ByteArrays{
        public static int encodedLength(byte[] array){
            return 4+array.length;
        }
        public static void encode(ByteBuf buf, byte[] arr){
            buf.writeInt(arr.length);
            buf.writeBytes(arr);
        }
        public static byte[] decode(ByteBuf buf){
            int length = buf.readInt();
            byte[] bytes = new byte[length];
            buf.readBytes(bytes);
            return bytes;
        }
    }
    public static class StringArrays{
        public static int encodedLength(String[] strings){
            int totalLength=4;
            for (String s : strings) {
                totalLength+=Strings.encodedLength(s);
            }
            return totalLength;
        }
        public static void encode(ByteBuf buf, String[] strings){
            buf.writeInt(strings.length);
            for (String s : strings) {
                Strings.encode(buf,s);
            }
        }
        public static String[] decode(ByteBuf buf){
            int numStrings = buf.readInt();
            String[] strings = new String[numStrings];
            for (int i = 0; i < strings.length; i++) {
                strings[i]=Strings.decode(buf);
            }
            return strings;
        }
    }
    public static class IntArrays{
        public static int encodedLength(int[] ints){
            return 4+4*ints.length;
        }
        public static void encode(ByteBuf buf, int[] ints){
            buf.writeInt(ints.length);
            for (int i : ints) {
                buf.writeInt(i);
            }
        }
        public static int[] decode(ByteBuf buf){
            int numInts = buf.readInt();
            int[] ints = new int[numInts];
            for (int i = 0; i < ints.length; i++) {
                ints[i]=buf.readInt();
            }
            return ints;
        }
    }
    public static class LongArrays{
        public static int encodedLength(long[] longs){
            return 4+8*longs.length;
        }
        public static void encode(ByteBuf buf, long[] longs){
            buf.writeInt(longs.length);
            for (long i : longs) {
                buf.writeLong(i);
            }
        }
        public static long[] decode(ByteBuf buf){
            int numInts = buf.readInt();
            long[] longs = new long[numInts];
            for (int i = 0; i < longs.length; i++) {
                longs[i]=buf.readLong();
            }
            return longs;
        }
    }
    public static class BitmapArrays{
        public static int encodedLength(RoaringBitmap[] bitmaps){
            int totalLength =4;
            for (RoaringBitmap b : bitmaps) {
                totalLength +=Bitmaps.encodedLength(b);
            }
            return totalLength;
        }
        public static void encode(ByteBuf buf, RoaringBitmap[] bitmaps){
            buf.writeInt(bitmaps.length);
            for (RoaringBitmap b : bitmaps) {
                Bitmaps.encode(buf,b);
            }
        }
        public static RoaringBitmap[] decode(ByteBuf buf){
            int numBitmaps = buf.readInt();
            RoaringBitmap[] bitmaps = new RoaringBitmap[numBitmaps];
            for (int i = 0; i < bitmaps.length; i++) {
                bitmaps[i]=Bitmaps.decode(buf);
            }
            return bitmaps;
        }
    }

}
