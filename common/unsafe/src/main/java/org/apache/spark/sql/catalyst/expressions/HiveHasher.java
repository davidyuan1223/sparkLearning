package org.apache.spark.sql.catalyst.expressions;

public class HiveHasher {
    @Override
    public String toString() {
        return HiveHasher.class.getSimpleName();
    }
    public static int hashInt(int input){
        return input;
    }
    public static int hasLong(long input){
        return (int) ((input >>> 32) ^ input);
    }
    public static int hashUnsafeBytes(Object base, long offset, int lengthInBytes) {
        assert (lengthInBytes>=0): "lengthInBytes cannot be negative";
        int result=0;
        for (int i = 0; i < lengthInBytes; i++) {
            result=(result*31)+(int) Platform.getByte(base,offset+i);
        }
        return result;
    }
}
