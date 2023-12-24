package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

import java.util.*;

public class LocalDirsForExecutors extends BlockTransferMessage{
    private final String[] execIds;
    private final int[] numLocalDirsByExec;
    private final String[] allLocalDirs;

    public LocalDirsForExecutors(Map<String, String[]> localDirsByExec) {
        this.execIds = new String[localDirsByExec.size()];
        this.numLocalDirsByExec = new int[localDirsByExec.size()];
        ArrayList<String> localDirs = new ArrayList<>();
        int index = 0;
        for (Map.Entry<String, String[]> e: localDirsByExec.entrySet()) {
            execIds[index] = e.getKey();
            numLocalDirsByExec[index] = e.getValue().length;
            Collections.addAll(localDirs, e.getValue());
            index++;
        }
        this.allLocalDirs = localDirs.toArray(new String[0]);
    }

    private LocalDirsForExecutors(String[] execIds, int[] numLocalDirsByExec, String[] allLocalDirs) {
        this.execIds = execIds;
        this.numLocalDirsByExec = numLocalDirsByExec;
        this.allLocalDirs = allLocalDirs;
    }

    @Override
    protected Type type() { return Type.LOCAL_DIRS_FOR_EXECUTORS; }

    @Override
    public int hashCode() {
        return Arrays.hashCode(execIds);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("execIds", Arrays.toString(execIds))
                .append("numLocalDirsByExec", Arrays.toString(numLocalDirsByExec))
                .append("allLocalDirs", Arrays.toString(allLocalDirs))
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof LocalDirsForExecutors) {
            LocalDirsForExecutors o = (LocalDirsForExecutors) other;
            return Arrays.equals(execIds, o.execIds)
                    && Arrays.equals(numLocalDirsByExec, o.numLocalDirsByExec)
                    && Arrays.equals(allLocalDirs, o.allLocalDirs);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.StringArrays.encodedLength(execIds)
                + Encoders.IntArrays.encodedLength(numLocalDirsByExec)
                + Encoders.StringArrays.encodedLength(allLocalDirs);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.StringArrays.encode(buf, execIds);
        Encoders.IntArrays.encode(buf, numLocalDirsByExec);
        Encoders.StringArrays.encode(buf, allLocalDirs);
    }

    public static LocalDirsForExecutors decode(ByteBuf buf) {
        String[] execIds = Encoders.StringArrays.decode(buf);
        int[] numLocalDirsByExec = Encoders.IntArrays.decode(buf);
        String[] allLocalDirs = Encoders.StringArrays.decode(buf);
        return new LocalDirsForExecutors(execIds, numLocalDirsByExec, allLocalDirs);
    }

    public Map<String, String[]> getLocalDirsByExec() {
        Map<String, String[]> localDirsByExec = new HashMap<>();
        int index = 0;
        int localDirsIndex = 0;
        for (int length: numLocalDirsByExec) {
            localDirsByExec.put(execIds[index],
                    Arrays.copyOfRange(allLocalDirs, localDirsIndex, localDirsIndex + length));
            localDirsIndex += length;
            index++;
        }
        return localDirsByExec;
    }
}
