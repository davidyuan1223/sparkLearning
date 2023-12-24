package org.apache.spark.network.util;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

public abstract class AbstractFileRegion extends AbstractReferenceCounted implements FileRegion {
    @Override
    public long transfered() {
        return transferred();
    }

    @Override
    public AbstractFileRegion retain() {
        super.retain();
        return this;
    }

    @Override
    public AbstractFileRegion retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public AbstractFileRegion touch(Object o) {
        return this;
    }

    @Override
    public AbstractFileRegion touch() {
        super.touch();
        return this;
    }
}
