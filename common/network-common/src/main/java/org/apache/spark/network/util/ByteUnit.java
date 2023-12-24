package org.apache.spark.network.util;

public enum ByteUnit {
    BYTE(1),
    KiB(1L<<10),
    MiB(1L<<20),
    GiB(1L<<30),
    TiB(1L<<40),
    PiB(1L<<50);
    ByteUnit(long multiplier){
        this.multiplier=multiplier;
    }
    public long convertFrom(long d, ByteUnit u){
        return u.convertTo(d,this);
    }
    public long convertTo(long d,ByteUnit u){
        if (multiplier > u.multiplier) {
            long ratio = multiplier / u.multiplier;
            if (Long.MAX_VALUE / ratio < d) {
                throw new IllegalArgumentException("Conversion of "+d+" exceeds Long.MAX_VALUE in " +
                        name()+". Try a larger unit (e.g. MiB instead of Kib)");
            }
            return d*ratio;
        }else {
            return d/(u.multiplier/multiplier);
        }
    }

    public long toBytes(long d){
        if (d < 0) {
            throw new IllegalArgumentException("Negative size value. Size must be positive: "+d);
        }
        return d*multiplier;
    }

    public long toKib(long d){
        return convertTo(d,KiB);
    }
    public long toMib(long d){
        return convertTo(d,MiB);
    }
    public long toGib(long d){
        return convertTo(d,GiB);
    }
    public long toTib(long d){
        return convertTo(d,TiB);
    }
    public long toPib(long d){
        return convertTo(d,PiB);
    }
    private final long multiplier;
}
