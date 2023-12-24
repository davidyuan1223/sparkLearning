package org.apache.spark.unsafe;

import org.apache.spark.unsafe.array.ByteArrayMethods;

public class UTF8StringBuilder {
    private static final int ARRAY_MAX= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;
    private byte[] buffer;
    private int cursor = Platform.BYTE_ARRAY_OFFSET;

    public UTF8StringBuilder() {
        // Since initial buffer size is 16 in `StringBuilder`, we set the same size here
        this(16);
    }

    public UTF8StringBuilder(int initialSize) {
        if (initialSize < 0) {
            throw new IllegalArgumentException("Size must be non-negative");
        }
        if (initialSize > ARRAY_MAX) {
            throw new IllegalArgumentException(
                    "Size " + initialSize + " exceeded maximum size of " + ARRAY_MAX);
        }
        this.buffer = new byte[initialSize];
    }

    // Grows the buffer by at least `neededSize`
    private void grow(int neededSize) {
        if (neededSize > ARRAY_MAX - totalSize()) {
            throw new UnsupportedOperationException(
                    "Cannot grow internal buffer by size " + neededSize + " because the size after growing " +
                            "exceeds size limitation " + ARRAY_MAX);
        }
        final int length = totalSize() + neededSize;
        if (buffer.length < length) {
            int newLength = length < ARRAY_MAX / 2 ? length * 2 : ARRAY_MAX;
            final byte[] tmp = new byte[newLength];
            Platform.copyMemory(
                    buffer,
                    Platform.BYTE_ARRAY_OFFSET,
                    tmp,
                    Platform.BYTE_ARRAY_OFFSET,
                    totalSize());
            buffer = tmp;
        }
    }

    private int totalSize() {
        return cursor - Platform.BYTE_ARRAY_OFFSET;
    }

    public void append(UTF8String value) {
        grow(value.numBytes());
        value.writeToMemory(buffer, cursor);
        cursor += value.numBytes();
    }

    public void append(String value) {
        append(UTF8String.fromString(value));
    }

    public void appendBytes(Object base, long offset, int length) {
        grow(length);
        Platform.copyMemory(
                base,
                offset,
                buffer,
                cursor,
                length);
        cursor += length;
    }

    public UTF8String build() {
        return UTF8String.fromBytes(buffer, 0, totalSize());
    }
}
