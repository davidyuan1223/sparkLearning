package org.apache.spark.memory;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskMemoryManager {
    private static final Logger logger = LoggerFactory.getLogger(TaskMemoryManager.class);
    // The number of bits used to address the page table
    private static final int PAGE_NUMBER_BITS=13;
    // The number of bits used to encode offsets in data pages
    @VisibleForTesting
    static final int OFFSET_BITS=64-PAGE_NUMBER_BITS;
    // The number of entries in the page table
    private static final int PAGE_TABLE_SIZE=1<<PAGE_NUMBER_BITS;
    /**
     * Maximum supported data page size (in bytes). In principle, the maximum addressable page size is
     * (1L<<OFFSET_BITS) bytes, which is 2+ petabytes. However, the on-heap allocator's maximum page size
     * is limited by the maximum amount of data that can be stored in a long[] array, which is (2^31-1)*8 bytes
     * (or about 17 gigabytes). Therefore, we cap this at 17 gigabytes.
     */
    public static final long MAXIMUM_PAGE_SIZE_BYTES=((1L<<31)-1)*8L;
    // Bit mask for the lower 51 bits of a long
    private static final long MASK_LONG_LOWER_51_BITES=0x7FFFFFFFFFFFFL;
    private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

}
