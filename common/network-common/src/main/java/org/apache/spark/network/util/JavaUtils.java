package org.apache.spark.network.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.SystemUtils;
import org.eclipse.jetty.util.IO;
import org.rocksdb.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaUtils {
    private static final Logger logger= LoggerFactory.getLogger(JavaUtils.class);
    public static final long DEFAULT_DRIVER_MEM_MB=1024;
    public static void closeQuietly(Closeable closeable){
        try {
            if (closeable != null) {
                closeable.close();
            }
        }catch (IOException e){
            logger.error("IOException should not have been thrown.", e);
        }
    }
    public static int nonNegativeHash(Object object){
        if (object == null) {
            return 0;
        }
        int hash = object.hashCode();
        return hash!=Integer.MIN_VALUE?Math.abs(hash):0;
    }

    public static ByteBuffer stringToBytes(String s){
        return Unpooled.wrappedBuffer(s.getBytes(StandardCharsets.UTF_8)).nioBuffer();
    }

    public static String bytesToString(ByteBuffer b){
        return Unpooled.wrappedBuffer(b).toString(StandardCharsets.UTF_8);
    }

    public static void deleteRecursively(File file)throws IOException{
        deleteRecursively(file, null);
    }

    public static void deleteRecursively(File file, FilenameFilter filter)throws IOException{
        if (file == null) {
            return;
        }
        if (SystemUtils.IS_OS_UNIX && filter == null) {
            try {
                deleteRecursivelyUsingUnixNative(file);
                return;
            }catch (IOException e){
                logger.warn("Attempt to delete using native Unix OS command failed for path={}. " +
                        "Falling back to Java IO way", file.getAbsolutePath(), e);
            }
        }
        deleteRecursivelyUsingJavaIO(file,filter);
    }

    private static void deleteRecursivelyUsingJavaIO(File file,FilenameFilter filter)throws IOException{
        BasicFileAttributes fileAttributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
        if (fileAttributes.isDirectory() && !isSymlink(file)) {
            IOException savedIOException = null;
            for (File child: listFilesSafely(file,filter)){
                try {
                    deleteRecursively(child,filter);
                }catch (IOException e){
                    savedIOException=e;
                }
            }
            if (savedIOException != null) {
                throw savedIOException;
            }
        }
        if (fileAttributes.isRegularFile() ||
                (fileAttributes.isDirectory() && listFilesSafely(file, null).length == 0)) {
            boolean delete = file.delete();
            if (!delete && file.exists()) {
                throw new IOException("Failed to delete: "+file.getAbsolutePath());
            }
        }
    }

    private static void deleteRecursivelyUsingUnixNative(File file)throws IOException{
        ProcessBuilder builder = new ProcessBuilder("rm", "-rf", file.getAbsolutePath());
        Process process=null;
        int exitCode=-1;
        try {
            builder.redirectErrorStream(true);
            builder.redirectOutput(new File("/dev/null"));
            process=builder.start();
            exitCode=process.waitFor();
        }catch (Exception e){
            throw new IOException("Failed to delete: "+file.getAbsolutePath(),e);
        }finally {
            if (process != null) {
                process.destroy();
            }
        }
        if (exitCode != 0 || file.exists()) {
            throw new IOException("Failed to delete: "+file.getAbsolutePath());
        }
    }

    private static File[] listFilesSafely(File file, FilenameFilter filter)throws IOException{
        if (file.exists()) {
            File[] files = file.listFiles(filter);
            if (files == null) {
                throw new IOException("Failed to list files for dir: "+file);
            }
            return files;
        }else {
            return new File[0];
        }
    }

    private static boolean isSymlink(File file)throws IOException{
        Preconditions.checkNotNull(file);
        File fileInCanonicalDir=null;
        if (file.getParent() == null) {
            fileInCanonicalDir=file;
        }else {
            fileInCanonicalDir=new File(file.getParentFile().getCanonicalFile(),file.getName());
        }
        return !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile());
    }

    private static final ImmutableMap<String , TimeUnit> timeSuffixes =
            ImmutableMap.<String,TimeUnit>builder()
            .put("us",TimeUnit.MICROSECONDS)
            .put("ms",TimeUnit.MILLISECONDS)
            .put("s",TimeUnit.SECONDS)
            .put("m",TimeUnit.MINUTES)
            .put("min",TimeUnit.MINUTES)
            .put("h",TimeUnit.HOURS)
            .put("d",TimeUnit.DAYS)
            .build();

    private static final ImmutableMap<String , ByteUnit> byteSuffixes =
            ImmutableMap.<String, ByteUnit>builder()
            .put("b",ByteUnit.BYTE)
            .put("k",ByteUnit.KiB)
            .put("kb",ByteUnit.KiB)
            .put("m",ByteUnit.MiB)
            .put("mb",ByteUnit.MiB)
            .put("g",ByteUnit.GiB)
            .put("gb",ByteUnit.GiB)
            .put("t",ByteUnit.TiB)
            .put("tb",ByteUnit.TiB)
            .put("p",ByteUnit.PiB)
            .put("pb",ByteUnit.PiB)
            .build();

    public static long timeStringAs(String str,TimeUnit unit){
        String lower = str.toLowerCase(Locale.ROOT).trim();
        try {
            Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
            if (!m.matches()) {
                throw new NumberFormatException("Failed to parse time string: "+str);
            }
            long val = Long.parseLong(m.group(1));
            String suffix=m.group(2);
            if (suffix != null && !timeSuffixes.containsKey(suffix)) {
                throw new NumberFormatException("Invalid suffix: \""+suffix+"\"");
            }
            return unit.convert(val,suffix!=null?timeSuffixes.get(suffix):unit);
        }catch (NumberFormatException e){
            String timeError="Time must be specified as seconds (s), " +
                    "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). " +
                    "E.g. 50s, 100ms, or 250us.";
            throw new NumberFormatException(timeError+"\n"+e.getMessage());
        }
    }

    public static long timeStringAsSec(String str){
        return timeStringAs(str, TimeUnit.SECONDS);
    }
    public static long timeStringAsMs(String str){
        return timeStringAs(str, TimeUnit.MILLISECONDS);
    }

    public static long byteStringAs(String str, ByteUnit unit) {
        String lower = str.toLowerCase(Locale.ROOT).trim();
        try {
            Matcher m = Pattern.compile("([0-9]+)([a-z]+)?").matcher(lower);
            Matcher fractionMatcher = Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?").matcher(lower);
            if (m.matches()) {
                long val = Long.parseLong(m.group(1));
                String suffix = m.group(2);
                if (suffix != null && !byteSuffixes.containsKey(suffix)) {
                    throw new NumberFormatException("Invalid suffix: \""+suffix+"\"");
                }
                return unit.convertTo(val,suffix!=null?byteSuffixes.get(suffix):unit);
            } else if (fractionMatcher.matches()) {
                throw new NumberFormatException("Fractional values are not supported. Input was: " +
                        fractionMatcher.group(1));
            }else {
                throw new NumberFormatException("Failed to parse byte string: "+str);
            }
        }catch (NumberFormatException e){
            String byteError = "Size must be specified as bytes (b), " +
                    "kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). " +
                    "E.g. 50b, 100k, or 250m.";

            throw new NumberFormatException(byteError + "\n" + e.getMessage());
        }
    }

    public static long byteStringAsBytes(String str){
        return byteStringAs(str, ByteUnit.BYTE);
    }
    public static long byteStringAsKb(String str){
        return byteStringAs(str, ByteUnit.KiB);
    }
    public static long byteStringAsMb(String str){
        return byteStringAs(str, ByteUnit.MiB);
    }
    public static long byteStringAsGb(String str){
        return byteStringAs(str, ByteUnit.GiB);
    }
    public static byte[] bufferToArray(ByteBuffer buffer){
        if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.array().length == buffer.remaining()) {
            return buffer.array();
        }else {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
    }
    public static File createTempDir()throws IOException{
        return createTempDir(System.getProperty("java.io.tmpdir"),"spark");
    }
    public static File createTempDir(String root, String namePrefix)throws IOException{
        if (root == null) {
            root=System.getProperty("java.io.tempdir");
        }
        if (namePrefix == null) {
            namePrefix="spark";
        }
        File dir=createDirectory(root,namePrefix);
        dir.deleteOnExit();
        return dir;
    }

    public static File createDirectory(String root)throws IOException{
        return createDirectory(root, "spark");
    }
    public static File createDirectory(String root, String namePrefix)throws IOException{
        if (namePrefix == null) {
            namePrefix="spark";
        }
        int attempts=0;
        int maxAttempts=10;
        File dir=null;
        while (dir == null) {
            attempts+=1;
            if (attempts > maxAttempts) {
                throw new IOException("Failed to create a temp directory (under "+root+") after " +
                        maxAttempts+" attempts!");
            }
            try {
                dir=new File(root,namePrefix+"-"+ UUID.randomUUID());
                Files.createDirectories(dir.toPath());
            }catch (IOException | SecurityException e){
                logger.error("Failed to create directory "+dir,e);
                dir=null;
            }
        }
        return dir.getCanonicalFile();
    }
    public static void readFully(ReadableByteChannel channel, ByteBuffer dst)throws IOException{
        int expected = dst.remaining();
        while (dst.hasRemaining()) {
            if (channel.read(dst) < 0) {
                throw new EOFException(String.format("Not enough bytes in channel (expected %d).",expected));
            }
        }
    }
}
