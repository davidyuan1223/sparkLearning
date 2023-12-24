package org.apache.spark.network.shuffle;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.util.TransportConf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class SimpleDownloadFile implements DownloadFile{
    private final File file;
    private final TransportConf transportConf;
    public SimpleDownloadFile(File file, TransportConf transportConf){
        this.file=file;
        this.transportConf=transportConf;
    }

    @Override
    public boolean delete() {
        return file.delete();
    }

    @Override
    public DownloadFileWritableChannel openForWriting() throws IOException {
        return new SimpleDownloadWritableChannel();
    }

    @Override
    public String path() {
        return file.getAbsolutePath();
    }


    private class SimpleDownloadWritableChannel implements DownloadFileWritableChannel{
        private final WritableByteChannel channel;
        SimpleDownloadWritableChannel() throws FileNotFoundException{
            channel= Channels.newChannel(new FileOutputStream(file));
        }

        @Override
        public ManagedBuffer closeAndRead() {
            return new FileSegmentManagedBuffer(transportConf,file,0,file.length());
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return channel.write(src);
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
