package org.apache.spark.network.shuffle.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;
import java.util.Objects;

public class ExecutorShuffleInfo implements Encodable {
    public final String[] localDirs;
    public final int subDirsPerLocalDir;
    public final String shuffleManager;

    @JsonCreator
    public ExecutorShuffleInfo(
            @JsonProperty("localDirs") String [] localDirs,
            @JsonProperty("subDirsPerLocalDir") int subDirsPerLocalDir,
            @JsonProperty("shuffleManager") String shuffleManager
    ){
        this.localDirs=localDirs;
        this.subDirsPerLocalDir=subDirsPerLocalDir;
        this.shuffleManager=shuffleManager;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subDirsPerLocalDir, shuffleManager) * 41 + Arrays.hashCode(localDirs);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("localDirs", Arrays.toString(localDirs))
                .append("subDirsPerLocalDir", subDirsPerLocalDir)
                .append("shuffleManager", shuffleManager)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ExecutorShuffleInfo) {
            ExecutorShuffleInfo o = (ExecutorShuffleInfo) other;
            return Arrays.equals(localDirs, o.localDirs)
                    && subDirsPerLocalDir == o.subDirsPerLocalDir
                    && Objects.equals(shuffleManager, o.shuffleManager);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.StringArrays.encodedLength(localDirs)
                +4
                +Encoders.Strings.encodedLength(shuffleManager);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.StringArrays.encode(buf,localDirs);
        buf.writeInt(subDirsPerLocalDir);
        Encoders.Strings.encode(buf,shuffleManager);
    }

    public static ExecutorShuffleInfo decode(ByteBuf buf) {
        String [] localDirs = Encoders.StringArrays.decode(buf);
        int subDirsPerLocalDir = buf.readInt();
        String shuffleManager = Encoders.Strings.decode(buf);
        return new ExecutorShuffleInfo(localDirs,subDirsPerLocalDir,shuffleManager);
    }

}
