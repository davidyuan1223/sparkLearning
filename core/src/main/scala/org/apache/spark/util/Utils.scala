package org.apache.spark.util

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.collect.Interners
import org.apache.commons.io.output.ByteArrayOutputStream
import com.google.common.io.{ByteStreams, Files => GFiles}
import com.google.common.net.InetAddresses
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.util.{RunJar, StringUtils}
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Tests._
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

import java.io.{ByteArrayInputStream, DataOutput, File, FileInputStream, FileNotFoundException, FileOutputStream, IOException, InputStream, ObjectInputStream, ObjectOutputStream, ObjectStreamClass, OutputStream, RandomAccessFile, SequenceInputStream}
import java.lang
import java.lang.reflect.InvocationTargetException
import java.math.{MathContext, RoundingMode}
import java.net.{InetAddress, MalformedURLException, NetworkInterface, URI, URISyntaxException, URL}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, FileChannel}
import java.nio.file.Files
import java.util.Locale
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.zip.GZIPInputStream
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, enumerationAsScalaIteratorConverter}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{DAYS, NANOSECONDS}
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.control.{ControlThrowable, NonFatal}
import scala.util.{Random, Try}
import org.apache.spark.util.collection.{Utils => CUtils}

private [spark] case class CallSite(shortForm: String, longForm: String)

private [spark] object CallSite{
  val SHORT_FORM = "callSite.short"
  val LONG_FORM = "callSite.long"
  val empty = CallSite("", "")
}

private [spark] object Utils extends Logging {
  val random = new Random()
  private val sparkUncaughtExceptionHandler = new SparkUncaughtExceptionHandler
  @volatile private var cachedLocalDir: String = ""
  val DEFAULT_DRIVER_MEM_MB = JavaUtils.DEFAULT_DRIVER_MEM_MB.toInt
  val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  @volatile private var localRootDirs: Array[String] = null
  val LOCAL_SCHEMA = "local"
  private val weakStringInterner = Interners.newStrongInterner[String]()
  private val PATTERN_FOR_COMMAND_LINE_ARG = "-D(.+?)=(.+)".r
  private val COPY_BUFFER_LEN = 1024
  private val copyBuffer = ThreadLocal.withInitial[Array[Byte]](()=>{
    new Array[Byte](COPY_BUFFER_LEN)
  })
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        Class.forName(desc.getName,false,loader)
      }
    }
    ois.readObject.asInstanceOf[T]
  }
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }
  def deserializeLongValue(bytes: Array[Byte]): Long = {
    var result = bytes(7)&0xFFL
    result = result + ((bytes(6) & 0xFFL) << 8)
    result = result + ((bytes(5) & 0xFFL) << 16)
    result = result + ((bytes(4) & 0xFFL) << 24)
    result = result + ((bytes(3) & 0xFFL) << 32)
    result = result + ((bytes(2) & 0xFFL) << 40)
    result = result + ((bytes(1) & 0xFFL) << 48)
    result + ((bytes(0) & 0xFFL) << 56)
  }

  def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)
                              (f: SerializationStream => Unit): Unit = {
    val osWrapper = ser.serializeStream(new OutputStream {
      override def write(b: Int): Unit = os.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    })
    try {
      f(osWrapper)
    }finally {
      osWrapper.close()
    }
  }

  def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)
                                (f: DeserializationStream => Unit): Unit = {
    val isWrapper = ser.deserializeStream(new InputStream {
      override def read(): Int = is.read()
      override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b,off,len)
    })
    try {
      f(isWrapper)
    }finally {
      isWrapper.close()
    }
  }

  def weakIntern(s: String): String = {
    weakStringInterner.intern(s)
  }

  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  def classIsLoadable(clazz: String): Boolean = {
    Try { classForName(clazz, initialize = false) }.isSuccess
  }

  def classForName[C](className: String, initialize: Boolean = true,
                      noSparkClassLoader: Boolean = false): Class[C] = {
    if (!noSparkClassLoader) {
      Class.forName(className,initialize,getContextOrSparkClassLoader).asInstanceOf[Class[C]]
    }else {
      Class.forName(className,initialize,Thread.currentThread().getContextClassLoader)
        .asInstanceOf[Class[C]]
    }
  }

  def withContextClassLoader[T](ctxClassLoader: ClassLoader)(fn: => T): T = {
    def oldClassLoader = Thread.currentThread().getContextClassLoader
    try{
      Thread.currentThread().setContextClassLoader(ctxClassLoader)
      fn
    }finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader)
    }
  }

  private def writeByteBufferImpl(bb: ByteBuffer, writer: (Array[Byte], Int, Int) => Unit): Unit = {
    if (bb.hasArray){
      writer(bb.array(), bb.arrayOffset()+bb.position(), bb.remaining())
    }else {
      val buffer = {
        copyBuffer.get()
      }
      val originalPosition = bb.position()
      var bytesToCopy = Math.min(bb.remaining(), COPY_BUFFER_LEN)
      while (bytesToCopy > 0) {
        bb.get(buffer, 0 ,bytesToCopy)
        writer(buffer, 0, bytesToCopy)
        bytesToCopy = Math.min(bb.remaining(), COPY_BUFFER_LEN)
      }
      bb.position(originalPosition)
    }
  }

  def writeByteBuffer(bb: ByteBuffer, out: DataOutput): Unit = {
    writeByteBufferImpl(bb, out.write)
  }

  def writeByteBuffer(bb: ByteBuffer, out: OutputStream): Unit = {
    writeByteBufferImpl(bb, out.write)
  }

  def chmod700(file: File): Boolean = {
    file.setReadable(false, false)&&
      file.setReadable(true, true)&&
      file.setWritable(false,false)&&
      file.setWritable(true,true)&&
      file.setExecutable(false,false)&&
      file.setExecutable(true,true)
  }

  def createDirectory(dir: File): Boolean = {
    try{
      Files.createDirectories(dir.toPath)
      if (!dir.exists() || !dir.isDirectory) {
        logError(s"Failed to create director $dir")
      }
      dir.isDirectory
    }catch {
      case e: Exception =>
        logError(s"Failed to create director $dir",e)
        false
    }
  }

  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    JavaUtils.createDirectory(root, namePrefix)
  }

  def createTempDir(root: String = System.getProperty("java.io.tmpdir"),
                    namePrefix: String = "spark"): File = {
    JavaUtils.createTempDir(root, namePrefix)
  }

  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false,
                 transferToEnabled: Boolean = false): Long = {
    tryWithSafeFinally {
      (in, out) match {
        case (input: FileInputStream, output: FileOutputStream) if transferToEnabled =>
          val inChannel = input.getChannel
          val outChannel = output.getChannel
          val size = inChannel.size()
          copyFileStreamNIO(inChannel, outChannel, 0, size)
          size
        case (input, output) =>
          var count = 0L
          val buf = new Array[Byte](8192)
          var n=0
          while (n != -1){
            n=input.read(buf)
            if(n != -1){
              output.write(buf,0,n)
              count+=n
            }
          }
          count
      }
    } {
      if(closeStreams) {
        try {
          in.close()
        }finally {
          out.close()
        }
      }
    }
  }

  def copyStreamUpTo(in: InputStream, maxSize: Long): InputStream = {
    var count = 0L
    val out = new ChunkedByteBufferOutputStream(64*1024,ByteBuffer.allocate)
    val fullyCopied = tryWithSafeFinally {
      val bufSize=math.min(8192L,maxSize)
      val buf=new Array[Byte](bufSize.toInt)
      var n=0
      while (n != -1 && count<maxSize) {
        n=in.read(buf,0,math.min(maxSize-count,bufSize).toInt)
        if (n != -1){
          out.write(buf,0,n)
          count+=n
        }
      }
      count<maxSize
    } {
      try {
        if (count<maxSize) {
          in.close()
        }
      }finally {
        out.close()
      }
    }
    if (fullyCopied) {
      out.toChunkedByteBuffer.toInputStream(dispose = true)
    }else{
      new SequenceInputStream(out.toChunkedByteBuffer.toInputStream(dispose = true),in)
    }
  }

  def copyFileStreamNIO(input: FileChannel, output: FileChannel, startPosition: Int, bytesToCopy: Long): Unit = {
    val outputInitialState = output match {
      case outputFileChannel: FileChannel =>
        Some((outputFileChannel.position(),outputFileChannel))
      case _ => None
    }
    var count =0L
    while (count<bytesToCopy){
      count += input.transferTo(count+startPosition,bytesToCopy-count,output)
    }
    assert(count==bytesToCopy,s"request to copy $bytesToCopy bytes,but actually copied $count bytes.")
    outputInitialState.foreach{
      case (initialPos,outputFileChannel) =>
        val finalPos = outputFileChannel.position()
        val expectedPos = initialPos+bytesToCopy
        assert(finalPos==expectedPos,
          s"""
             |Current position $finalPos do not equal to expected position $expectedPos
             |after transferTo, please check your kernel version to see if it is 2.6.32,
             |this is a kernel bug which will lead to unexpected behavior when using transferTo.
             |You can set spark.file.transferTo = false to disable this NIO feature.
             |""".stripMargin)
    }
  }

  def encodeFileNameToURIRawPath(fileName: String): String = {
    require(!fileName.contains("/") && !fileName.contains("\\"))
    new URI("file",null,"localhost",-1,"/"+fileName,null,null).getRawPath.substring(1)
  }

  def decodeFileNameInURI(uri: URI): String = {
    val rawPath = uri.getRawPath
    val rawFileName = rawPath.split("/").last
    new URI("file:///"+rawFileName).getPath.substring(1)
  }

  def fetchFile(uri: String, targetDir: File,
                conf: SparkConf, hadoopConf: Configuration,
                timestamp: Long, useCache: Boolean,
                shouldUntar: Boolean=true): File ={
    val fileName = decodeFileNameInURI(new URI(uri))
    val targetFile = new File(targetDir,fileName)
    val fetchCacheEnabled = conf.getBoolean("spark.files.useFetchCache", defaultValue = true)
    if(useCache && fetchCacheEnabled) {
      val cachedFileName = s"${uri.hashCode}${timestamp}_cache"
      val lockFileName = s"${uri.hashCode}${timestamp}_lock"
      if (cachedLocalDir.isEmpty){
        this.synchronized{
          if (cachedLocalDir.isEmpty) {
            cachedLocalDir=getLocalDir(conf)
          }
        }
      }
      val localDir = new File(cachedLocalDir)
      val lockFile = new File(localDir, lockFileName)
      val lockFileChannel = new RandomAccessFile(lockFile,"rw").getChannel
      val lock = lockFileChannel.lock()
      val cachedFile = new File(localDir,cachedFileName)
      try{
        if(!cachedFile.exists()){
          doFetchFile(uri,localDir,cachedFileName,conf,hadoopConf)
        }
      }finally {
        lock.release()
        lockFileChannel.close()
      }
      copyFile(uri,cachedFile,targetFile,conf.getBoolean("spark.files.overwrite",false))
    }else{
      doFetchFile(url,targetFile,fileName,conf,hadoopConf)
    }

    if(shouldUntar){
      if (fileName.endsWith(".tar.gz") || fileName.endsWith(".tgz")) {
        logWarning(
          """
            |Untarring behavior will be deprecated at spark.files and
            |SparkContext.addFile. Consider using spark.archives or SparkContext.addArchive instead.
            |""".stripMargin)
        logInfo("Untarring "+fileName)
        executeAndgetOutput(Seq("tar","-xzf",fileName),targetDir)
      }else if(fileName.endsWith(".tar")){
        logWarning(
          """
            |Untarring behavior will be deprecated at spark.files and
            |SparkContext.addFile. Consider using spark.archives or SparkContext.addArchive instead.
            |""".stripMargin)
        logInfo("Untarring "+fileName)
        executeAndgetOutput(Seq("tar","-xf",fileName),targetDir)
      }
    }
    FileUtil.chmod(targetFile.getAbsolutePath,"a+x")
    if(isWindows){
      FileUtil.chmod(targetFile.getAbsolutePath,"u+r")
    }
    targetFile
  }

  def unpack(source: File, dest: File):Unit ={
    if (!source.exists()){
      throw new FileNotFoundException(source.getAbsolutePath)
    }
    val lowerSrc = StringUtils.toLowerCase(source.getName)
    if (lowerSrc.endsWith(".jar")) {
      RunJar.unJar(source,dest,RunJar.MATCH_ANY)
    }else if(lowerSrc.endsWith(".zip")){
      FileUtil.unZip(source,dest)
    }else if(lowerSrc.endsWith(".tar.gz") || lowerSrc.endsWith(".tgz")) {
      unTarUsingJava(source,dest)
    }else{
      logWarning(s"Cannot unpack $source, just copying it to $dest.")
      copyRecursive(source,dest)
    }
  }

  private def unTarUsingJava(source: File, dest: File): Unit = {
    if (!dest.mkdirs && !dest.isDirectory){
      throw new IOException(s"Mkdirs failed to create $dest")
    }else{
      try{
        val mth = classOf[FileUtil].getDeclaredMethod("untarUsingJava",classOf[File],classOf[File],classOf[Boolean])
        mth.setAccessible(true)
        mth.invoke(null,source,dest,java.lang.Boolean.FALSE)
      }catch {
        case e: InvocationTargetException if e.getCause!=null =>
          throw e.getCause
      }
    }
  }

  def timeTakenMs[T](body: => T): (T,Long) = {
    val startTime = System.nanoTime()
    val result = body
    val endTime = System.nanoTime()
    (result,math.max(NANOSECONDS.toMillis(endTime-startTime),0))
  }

  private def downloadFile(url: String,in: InputStream,
                           destFile: File, fileOverwrite: Boolean): Unit = {
    val tempFile = File.createTempFile("fetchFileTemp",null,new File(destFile.getParentFile.getAbsolutePath))
    logInfo(s"Fetching $url to $tempFile")
    try {
      val out = new FileOutputStream(tempFile)
      Utils.copyStream(in,out,closeStreams = true)
      copyFile(url,tempFile,destFile,fileOverwrite,removeSourceFile=true)
    }finally {
      if(tempFile.exists()){
        tempFile.delete()
      }
    }
  }

  private def copyFile(url: String, sourceFile: File,
                       destFile: File, fileOverwrite: Boolean,
                       removeSourceFile: Boolean = false): Unit = {
    if(destFile.exists()) {
      if (!filesEqualRecursive(sourceFile,destFile)) {
        if (fileOverwrite) {
          logInfo(s"File $destFile exist and does not match contents of $url, replacing it with $url")
          if(!destFile.delete()){
            throw new SparkException("Failed to delete %s while attempting to overwrite it with %s".format(
              destFile.getAbsolutePath,sourceFile.getAbsolutePath
            ))
          }
        }
      }else {
        throw new SparkException(s"File $destFile exists and does not match contents of $url")
      }
    }else {
      logInfo(s"${sourceFile.getAbsolutePath} has previously copied to ${destFile.getAbsolutePath}")
      return
    }
    if(removeSourceFile){
      Files.move(sourceFile.toPath,destFile.toPath)
    }else{
      logInfo(s"Copy ${sourceFile.getAbsolutePath} to ${destFile.getAbsolutePath}")
      copyRecursive(sourceFile,destFile)
    }
  }

  private def filesEqualRecursive(source: File, dest: File): Boolean = {
    if (source.isDirectory && dest.isDirectory) {
      val sourceSubFile = source.listFiles()
      val destSubFile = dest.listFiles()
      if (sourceSubFile.size != destSubFile.size) {
        return false
      }
      sourceSubFile.sortBy(_.getName).zip(destSubFile.sortBy(_.getName)).forall{
        case (f1,f2) => filesEqualRecursive(f1,f2)
      }
    }else if(source.isFile && dest.isFile) {
      GFiles.equal(source,dest)
    }else{
      false
    }
  }

  private def copyRecursive(source: File, dest: File): Unit = {
    if (source.isDirectory) {
      if (!dest.mkdir()) {
        throw new IOException(s"Failed to create directory ${dest.getPath}")
      }
      val subfiles = source.listFiles()
      subfiles.foreach(f => copyRecursive(f, new File(dest, f.getName)))
    } else {
      Files.copy(source.toPath, dest.toPath)
    }
  }

  def doFetchFile(url: String, targetDir: File,
                  filename: String, conf: SparkConf,
                  hadoopConf: Configuration): File = {
    val targetFile = new File(targetDir,filename)
    val uri = new URI(url)
    val fileOverwrite = conf.getBoolean("spark.files.overwrite", defaultValue=false)
    Option(uri.getScheme).getOrElse("file") match {
      case "spark" =>
        if (SparkEnv.get =null) {
          throw new IllegalStateException("Cannot retrieve files with 'spark' schema without an active SparkEnv")
        }
        val source = SparkEnv.get.rpcEnv.openChannel(url)
        val is = Channels.newInputStream(source)
        downloadFile(url,is,targetFile,fileOverwrite)
      case "http" | "https" | "ftp" =>
        val uc = new URL(url).openConnection()
        val timeoutMs =
          conf.getTimeAsSeconds("spark.files.fetchTimeout","60s").toInt * 1000
        uc.setConnectTimeout(timeoutMs)
        uc.setReadTimeout(timeoutMs)
        uc.connect()
        val in = uc.getInputStream
        downloadFile(url,in,targetFile,fileOverwrite)
      case "file" =>
        val sourceFile = if (uri.isAbsolute) new File(uri) else new File(uri.getPath)
        copyFile(url,sourceFile,targetFile,fileOverwrite)
      case _ =>
        val fs = getHadoopFileSystem(url,hadoopConf)
        val path = new Path(uri)
        fetchHcfsFile(path,targetDir,fs,conf,hadoopConf,fileOverwrite,filename = Some(filename))
    }
    targetFile
  }

  private [spark] def fetchHcfsFile(path: Path, targetDir: File, fs: FileSystem,
                                    conf: SparkConf, hadoopConf: Configuration, fileOverwrite: Boolean,
                                    filename: Option[String] = None): Unit = {
    if(!targetDir.exists() && !targetDir.mkdirs()) {
      throw new IOException(s"Failed to create directory ${targetDir.getPath}")
    }
    val dest = new File(targetDir,filename.getOrElse(path.getName))
    if (fs.getFileStatus(path).isFile){
      val in = fs.open(path)
      try {
        downloadFile(path.toString, in, dest,fileOverwrite)
      }finally {
        in.close()
      }
    }else {
      fs.listStatus(path).foreach { fileStatus =>
        fetchHcfsFile(fileStatus.getPath,dest,fs,conf, hadoopConf, fileOverwrite)
      }
    }
  }

  @throws[MalformedURLException]("When the URI is an invalid URL")
  def validateURL(uri: URI): Unit = {
    Option(uri.getScheme).getOrElse("file") match {
      case "http" | "https" | "ftp" =>
        try {
          uri.toURL
        }catch {
          case e: MalformedURLException =>
            val ex = new MalformedURLException(s"URI (${uri.toString}) is not a valid URL.")
            ex.initCause(e)
            throw ex
        }
      case _ =>
    }
  }

  def getLocalDir(conf: SparkConf): String = {
    val localRootDirs = getOrCreateLocalRootDirs(conf)
    if (localRootDirs.isEmpty) {
      val configuredLocalDirs = getConfiguredLocalDirs(conf)
      throw new IOException(s"Failed to get a temp directory under [${configuredLocalDirs.mkString(",")}].")
    }else{
      localRootDirs(scala.util.Random.nextInt(localRootDirs.length))
    }
  }

  private [spark] def isRunningInYarnContainer(conf: SparkConf): Boolean = {
    conf.getenv("CONTAINER_ID") != null
  }

  def isInRunningSparkTasks: Boolean = TaskContext.get() != null

  private [spark] def getOrCreateLocalRootDirs(conf: SparkConf): Array[String] = {
    if (localRootDirs == null){
      this.synchronized {
        if (localRootDirs == null) {
          localRootDirs = getOrCreateLocalRootDirsImpl(conf)
        }
      }
    }
    localRootDirs
  }

  def getConfiguredLocalDirs(conf: SparkConf): Array[String] = {
    val shuffleServiceEnabled = conf.get(config.SHUFFLE_SERVICE_ENABLED)
    if(isRunningInYarnContainer(conf)) {
      randomizeInPlace(getYarnLocalDirs(conf).split(","))
    }else if(conf.getenv("SPARK_EXECUTOR_DIRS") != null) {
      conf.getenv("SPARK_EXECUTOR_DIRS").split(File.pathSeparator)
    }else if(conf.getenv("SPARK_LOCAL_DIRS") != null) {
      conf.getenv("SPARK_LOCAL_DIRS").split(",")
    }else if(conf.getenv("MESOS_SANDBOX") != null && !shuffleServiceEnabled) {
      Array(conf.getenv("MESOS_SANDBOX"))
    }else {
      if(conf.getenv("MESOS_SANDBOX") !=null && shuffleServiceEnabled) {
        logInfo(s"MESOS_SANDBOX available but not using provided Mesos sandbox because ${config.SHUFFLE_SERVICE_ENABLED.key} is enabled")
      }
      conf.get("spark.local.dir",System.getProperty("java.io.tmpdir")).split(",")
    }
  }

  private def getOrCreateLocalRootDirsImpl(conf: SparkConf): Array[String] = {
    val configuredLocalDirs = getConfiguredLocalDirs(conf)
    val uris = configuredLocalDirs.filter{ root =>
      Try(new URI(root).getScheme!=null).getOrElse(false)
    }
    if (uris.nonEmpty) {
      logWarning(s"The configured local directories are not expected to be URIs; however, got suspicious " +
        s"values [${uris.mkString(", ")}]. Please check your configured local directories.")
    }
    configuredLocalDirs.flatMap{ root =>
      try {
        val rootDir = new File(root)
        if(rootDir.exists() || rootDir.mkdirs()){
          val dir = createTempDir(root)
          chmod700(dir)
          Some(dir.getAbsolutePath)
        }else{
          logError(s"Failed to create dir in $root. Ignoring this directory.")
          None
        }
      }catch {
        case e: IOException =>
          logError(s"Failed to create local root dir in $root. Ignoring this directory")
          None
      }
    }
  }

  private def getYarnLocalDirs(conf: SparkConf): String = {
    val localDirs = Option(conf.getenv("LOCAL_DIRS")).getOrElse("")
    if(localDirs.isEmpty){
      throw new Exception("Yarn Local dirs can't be empty")
    }
    localDirs
  }

  private [spark] def clearLocalRootDirs(): Unit = {
    localRootDirs = null
  }

  def randomize[T: ClassTag](seq: TraversableOnce[T]): Seq[T] ={
    randomizeInPlace(seq.toArray)
  }

  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for(i <- (arr.length-1) to 1 by -1) {
      val j =rand.nextInt(i+1)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  private lazy val localIpAddress: InetAddress = findLocalInetAddress()

  private def findLocalInetAddress(): InetAddress = {
    val defaultIpOverride = System.getenv("SPARK_LOCAL_IP")
    if(defaultIpOverride!=null){
      InetAddress.getByName(defaultIpOverride)
    }else{
      val address=InetAddress.getLocalHost
      if(address.isLoopbackAddress){
        val activeNetworkIFs  = NetworkInterface.getNetworkInterfaces.asScala.toSeq
        val reOrderedNetworkIFs = if(isWindows) activeNetworkIFs else activeNetworkIFs.reverse
        for (ni <- reOrderedNetworkIFs) {
          val addresses = ni.getInetAddresses.asScala
            .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
          if(addresses.nonEmpty){
            val addr=addresses.find(_.isInstanceOf[InetAddress]).getOrElse(addresses.head)
            val strippedAddress = InetAddress.getByAddress(addr.getAddress)
            logWarning(s"Your hostname, ${InetAddress.getLocalHost.getHostName} resolves to" +
              s"a loopback address: ${address.getHostAddress}; using ${strippedAddress.getHostAddress} " +
              s"instead (on interface ${ni.getName})")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return strippedAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address
    }
  }

  private var customHostname: Option[String] = sys.env.get("SPARK_LOCAL_HOSTNAME")
  def setCustomHostname(hostname: String): Unit = {
    Utils.checkHost(hostname)
    customHostname=Some(hostname)
  }
  def localCanonicalHostName(): String={
    addBracketsIfNeeded(customHostname.getOrElse(localIpAddress.getCanonicalHostName))
  }
  def localHostName(): String = {
    addbracketsIfNeeded(customHostname.getOrElse(localIpAddress.getHostAddress))
  }
  def localHostnameForURI(): String={
    addBracketsIfNeeded(customHostname.getOrElse(InetAddresses.toUriString(localIpAddress)))
  }
  private [spark] def addBrackestIfNeeded(addr: String): String = {
    if(addr.contains(":") && !addr.contains("[")) {
      "["+addr+"]"
    }else{
      addr
    }
  }
  private [spark] def normalizeIpIfNeeded(host: String): String = {
    val addressRe = """^\[{0,1}([0-9:]+?:[0-9]*)\]{0,1}$""".r
    host match {
      case addressRe(unbracketed) =>
        addBrackestIfNeeded(InetAddresses.toAddrString(InetAddresses.forString(unbracketed)))
      case _ =>
        host
    }
  }
  def checkHost(hostPort: String): Unit = {
    if (hostPort !=null && hostPort.split(":").length>2){
      assert(hostPort!=null && hostPort.indexOf("]:") != -1,s"Expected host and port but got $hostPort")
    }else{
      assert(hostPort!=null && hostPort.indexOf(':') != -1,s"Expected host and port but got $hostPort")
    }
  }
  private val hostPortParseResults = new ConcurrentHashMap[String,(String,Int)]()
  def parseHostPort(hostPort: String): (String, Int) = {
    val cached = hostPortParseResults.get(hostPort)
    if(cached != null){
      return cached
    }
    def setDefaultProtValue: (String,Int) = {
      val retval = (hostPort,0)
      hostPortParseResults.put(hostPort,return )
      retval
    }
    if(hostPort !=null && hostPort.split(":").length>2){
      val index: Int = hostPort.lastIndexOf("]:")
      if (-1 == index) {
        return setDefaultProtValue
      }
      val port = hostPort.substring(index+2).trim
      val retval = (hostPort.substring(0,index+1).trim,if(port.isEmpty) 0 else port.toInt)
      hostPortParseResults.putIfAbsent(hostPort,retval)
    }else {
      val index = hostPort.lastIndexOf(':')
      if(-1 == index) {
        return setDefaultProtValue
      }
      val port = hostPort.substring(index+1).trim
      val retval = (hostPort.substring(0,index).trim,if(port.isEmpty) 0 else port.toInt)
      hostPortParseResults.putIfAbsent(hostPort,retval)
    }
    hostPortParseResults.get(hostPort)
  }

  def getUsedTimeNs(startTimeNs: Long): String = {
    s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-startTimeNs)} ms"
  }

  def recursiveList(f: File): Array[File] = {
    require(f.isDirectory)
    val result = f.listFiles.toBuffer
    val dirList = result.filter(_.isDirectory)
    while (dirList.nonEmpty){
      val curDir = dirList.remove(0)
      val files = curDir.listFiles()
      result ++= files
      dirList ++= files.filter(_.isDirectory)
    }
    result.toArray
  }

  def deleteRecursively(file: File): Unit = {
    if (file!=null){
      JavaUtils.deleteRecursively(file)
      ShutdownHookManager.removeShutdownDeleteDir(file)
    }
  }

  def doesDirectoryContainAnyNewFiles(dir: File, cutoff: Long): Boolean = {
    if(!dir.isDirectory){
      throw new IllegalArgumentException(s"$dir is not a directory!")
    }
    val filesAndDirs = dir.listFiles()
    val cutoffTimeInMillis = System.currentTimeMillis()-(cutoff*1000)
    filesAndDirs.exists(_.lastModified()<cutoffTimeInMillis) ||
      filesAndDirs.filter(_.isDirectory).exists(
        subdir => doesDirectoryContainAnyNewFiles(subdir,cutoff)
      )
  }

  def timeStringAsMs(str: String): Long = {
    JavaUtils.timeStringAsMs(str)
  }

  def timeStringAsSeconds(str: String): Long = {
    JavaUtils.timeStringAsSec(str)
  }

  def byteStringAsBytes(str: String): Long = {
    JavaUtils.byteStringAsBytes(str)
  }

  def byteStringAsKb(str: String): Long = {
    JavaUtils.byteStringAsKb(str)
  }

  def byteStringAsMb(str: String): Long = {
    JavaUtils.byteStringAsMb(str)
  }

  def byteStringAsGb(str: String): Long = {
    JavaUtils.byteStringAsGb(str)
  }

  def memoryStringToMb(str: String): Int = {
    (JavaUtils.byteStringAsBytes(str)/1024/1024).toInt
  }

  private [this] val siByteSizes =
    Array(1L << 60, 1L << 50, 1L << 40, 1L << 30, 1L << 20, 1L << 10, 1)

  private [this] val siByteSuffixes =
    Array("EiB", "PiB", "TiB", "GiB", "MiB", "KiB", "B")

  def bytesToString(size: Long): String = {
    var i=0
    while (i<siByteSizes.length-1 && size<2*siByteSizes(i)) i+=1
    "%.1f %s".formatLocal(Locale.US, size.toDouble / siByteSizes(i), siByteSuffixes(i))
  }

  def bytesToString(size: BigInt): String = {
    val EiB = 1L << 60
    if(size.isValidLong) {
      bytesToString(size.toLong)
    }else if(size < BigInt(2L << 10) * EiB) {
      "%1f EiB".formatLocal(Locale.US, BigDecimal(size) / EiB)
    }else {
      BigDecimal(size, new MathContext(3,RoundingMode.HALF_UP)).toString()+" B"
    }
  }

  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60*second
    val hour = 60*minute
    val locale = Locale.US
    ms match {
      case t if t<second =>
        "%d ms".formatLocal(locale,t)
      case t if t<minute =>
        "%.1f s".formatLocal(locale,t.toFloat/second)
      case t if t<hour =>
        "%.1f m".formatLocal(locale,t.toFloat/minute)
      case t =>
        "%.2f h".formatLocal(locale,t.toFloat/hour)
    }
  }

  def megabytesToString(megabytes: Long): String = {
    bytesToString(megabytes*1024L*1024L)
  }

  def executeCommand(command: Seq[String],
                    workingDir: File = new File("."),
                     extraEnvironment: Map[String,String] = Map.empty,
                     redirectStderr: Boolean = true): Process = {
    val builder = new ProcessBuilder(command: _*).directory(workingDir)
    val environment = builder.environment()
    for((key, value) <- extraEnvironment) {
      environment.put(key,value)
    }
    val process = builder.start()
    if(redirectStderr){
      val threadName = "redirect stderr for command "+command(0)
      def log(s: String): Unit = logInfo(s)
      processStreamByLine(threadName,process.getErrorStream,log)
    }
    process
  }

  def executeAndGetOutput(command: Seq[String],
                          workingDir: File = new File("."),
                          extraEnvironment: Map[String,String] = Map.empty,
                          redirectStderr: Boolean = true): String = {
    val process = executeCommand(command,workingDir, extraEnvironment, redirectStderr)
    val output = new StringBuilder
    val threadName = "read stdout for "+command(0)
    def appendToOutput(s: String): Unit = output.append(s).append("\n")
    def stdoutThread = processStreamByLine(threadName,process.getInputStream,appendToOutput)
    val exitCode = process.waitFor()
    stdoutThread.join()
    if(exitCode != 0) {
      logError(s"process $command exited with $exitCode: $output")
      throw new SparkException(s"Process $command exited with code $exitCode")
    }
    output.toString()
  }

  def processStreamByLine(threadName: String,
                          inputStream: InputStream,
                          processLine: String => Unit): Thread = {
    val t = new Thread(threadName){
      override def run(): Unit = {
        for (line <- Source.fromInputStream(inputStream).getLines()){
          processLine(line)
        }
      }
    }
    t.setDaemon(true)
    t.start()
    t
  }

  def tryOrExit(block: => Unit): Unit = {
    try {
      block
    }catch {
      case e: ControlThrowable => throw e
      case t: Throwable => sparkUncaughtExceptionHandler.uncaughtException(t)
    }
  }

  def tryOrStopSparkContext(sc: SparkContext)(block: => Unit): Unit = {
    try {
      block
    }catch {
      case e: ControlThrowable => throw e
      case t: Throwable =>
        val currentThreadName = Thread.currentThread().getName
        if(sc != null) {
          logError(s"uncaught error in thread $currentThreadName, stopping SparkContext", t)
          sc.stopInNewThread()
        }
        if (!NonFatal(t)) {
          logError(s"throw uncaught fatal error in thread $currentThreadName",t)
          throw t
        }
    }
  }

  def tryOrIOException[T](block: => T): T = {
    try{
      block
    }catch {
      case e: IOException =>
        logError("Exception encountered",e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered",e)
        throw new IOException(e)
    }
  }

  def tryOrNonFatalError(block: => Unit): Unit = {
    try{
      block
    }catch {
      case NonFatal(t) =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}",t)
    }
  }

  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try{
      block
    }catch {
      case t: Throwable =>
        originalThrowable=t
        throw originalThrowable
    }finally {
      try {
        finallyBlock
      }catch {
        case t: Throwable if(originalThrowable !=null && originalThrowable!=t) =>
          originalThrowable.addSuppressed(t)
          logWarning(s"Suppressing exceptions in finally: ${t.getMessage}", t)
          throw originalThrowable
      }
    }
  }

  def tryWithSafeFinallyAndFailureCallbacks[T](block: => T)
                                              (catchBlock: => Unit=(), finallyBlock: => Unit = ()): T = {
    var originalThrowable: Throwable = null
    try {
      block
    }catch {
      case cause: Throwable =>
        originalThrowable=cause
        try {
          logError("Aborting task",originalThrowable)
          if (TaskContext.get() != null){
            TaskContext().get().markTaskFailed(originalThrowable)
          }
          catchBlock
        }catch {
          case t: Throwable =>
            if (originalThrowable != t) {
              originalThrowable.addSuppressed(t)
              logWarning(s"Suppressing exception in catch: ${t.getMessage}",t)
            }
        }
        throw originalThrowable
    }finally {
      try {
        finallyBlock
      }catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          logWarning(s"Suppressing exception in finally: ${t.getMessage}", t)
          throw originalThrowable
      }
    }
  }

  private val SPARK_CORE_CLASS_REGEX =
    """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]""".r
  private val SPARK_SQL_CLASS_REGEX =
    """^org\.apache\.spark\.sql.*""".r
  private def sparkInternalExclusionFunction(className: String): Boolean = {
    val SCALA_CORE_CLASS_PREFIX = "scala"
    val isSparkClass = SPARK_CORE_CLASS_REGEX.findFirstIn(className).isDefined ||
      SPARK_SQL_CLASS_REGEX.findFirstIn(className).isDefined
    val isScalaClass = className.startsWith(SCALA_CORE_CLASS_PREFIX)
    isSparkClass || isScalaClass
  }

  def getCallSite(skipClass: String => Boolean = sparkInternalExclusionFunction): CallSite = {
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var insideSpark = true
    val callStack = new ArrayBuffer[String]() :+ "<unknown>"

    Thread.currentThread.getStackTrace.foreach{ ste: StackTraceElement =>
      if(ste != null && ste.getMethodName != null
      && !ste.getMethodName.contains("getStackTrace")) {
        if(insideSpark) {
          if (skipClass(ste.getClassName)) {
            lastSparkMethod = if(ste.getMethodName == "<init>") {
              ste.getClassName.substring(ste.getClassName.lastIndexOf('.')+1)
            }else{
              ste.getMethodName
            }
            callStack(0) = ste.toString
          }else {
            if(ste.getFileName != null ){
              firstUserFile = ste.getFileName
              if(ste.getLineNumber >= 0){
                firstUserLine=ste.getLineNumber
              }
            }
            callStack += ste.toString
            insideSpark = false
          }
        }else {
          callStack += ste.toString
        }
      }
    }

    val callStackDepth = System.getProperty("spark.callstack.depth", "20").toInt
    val shortForm =
      if( firstUserFile == "HiveSessionImpl.java") {
        "Spark JDBC Server Query"
      }else {
        s"$lastSparkMethod at $firstUserFile:$firstUserLine"
      }
    val longForm = callStack.take(callStackDepth).mkString("\n")
    CallSite(shortForm, longForm)
  }

  private var compressedLogFileLengthCache: LoadingCache[String,java.lang.Long] = null
  private def getCompressedLogFileLengthCache(sparkConf: SparkConf): LoadingCache[String,java.lang.Long] = this.synchronized {
    if(compressedLogFileLengthCache == null) {
      val compressedLogFileLengthCacheSize = sparkConf.get(
        UNCOMPRESSED_LOG_FILE_LENGTH_CACHE_SIZE_CONF
      )
      compressedLogFileLengthCache = CacheBuilder.newBuilder()
        .maximumSize(compressedLogFileLengthCacheSize)
        .build[String,java.lang.Long](new CacheLoader[String,java.lang.Long]() {
          override def load(path: String): lang.Long = {
            Utils.getCompressedFileLength(new File(path))
          }
        })
    }
    compressedLogFileLengthCache
  }

  def getFileLength(file: File, workConf: SparkConf): Long = {
    if (file.getName.endsWith(".gz")){
      getCompressedLogFileLengthCache(workConf).get(file.getAbsolutePath)
    }else {
      file.length()
    }
  }

  private def getCompressedFileLength(file: File): Long = {
    var gzInputStream: GZIPInputStream = null
    try {
      var fileSize = 0L
      gzInputStream = new GZIPInputStream(new FileInputStream(file))
      val bufSize = 1024
      val buf = new Array[Byte](bufSize)
      var numBytes = ByteStreams.read(gzInputStream,buf,0,bufSize)
      while (numBytes > 0){
        fileSize+=numBytes
        numBytes=ByteStreams.read(gzInputStream,buf,0,bufSize)
      }
      fileSize
    }catch {
      case e: Throwable =>
        logError(s"Cannot get file length of ${file}", e)
        throw e
    }finally {
      if (gzInputStream != null){
        gzInputStream.close()
      }
    }
  }

  def offsetBytes(path: String, length: Long, start: Long, end: Long): String = {
    val file = new File(path)
    val effectiveEnd = math.min(length,end)
    val effectiveStart = math.max(0,start)
    val buff = new Array[Byte]((effectiveEnd-effectiveStart).toInt)
    val stream = if(path.endsWith(".gz")) {
      new GZIPInputStream(new FileInputStream(file))
    }else{
      new FileInputStream(file)
    }
    try{
      ByteStreams.skipFully(stream,effectiveStart)
      ByteStreams.readFully(stream,buff)
    }finally {
      stream.close()
    }
    Source.fromBytes(buff).mkString
  }

  def offsetBytes(files: Seq[File], fileLengths: Seq[Long],start: Long, end: Long): String = {
    assert(files.length == fileLengths.length)
    val startIndex = math.max(start,0)
    val endIndex = math.min(end,fileLengths.sum)
    val fileToLength = CUtils.toMap(files,fileLengths)
    logDebug(s"Log files: \n ${fileToLength.mkString("\n")}")
    val stringBuffer = new StringBuffer((endIndex-startIndex).toInt)
    var sum = 0L
    files.zip(fileLengths).foreach{case (file,fileLength) =>
      val startIndexOfFile = sum
      val endIndexOfFile = sum + fileToLength(file)
      logDebug(s"Processing file $file with start index=$startIndexOfFile, end index=$endIndexOfFile")
      /*
                                      ____________
       range 1:                      |            |
                                     |   case A   |

       files:   |==== file 1 ====|====== file 2 ======|===== file 3 =====|

                     |   case B  .       case C       .    case D    |
       range 2:      |___________.____________________.______________|
       */
      if(startIndex <= startIndexOfFile && endIndex >= endIndexOfFile){
        stringBuffer.append(offsetBytes(file.getAbsolutePath,fileLength,0,fileToLength(file)))
      }else if(startIndex > startIndexOfFile && startIndex < endIndexOfFile) {
        val effectiveStartIndex = startIndex-startIndexOfFile
        val effectiveEndIndex = math.min(endIndex-startIndexOfFile, fileToLength(file))
        stringBuffer.append(Utils.offsetBytes(file.getAbsolutePath,fileLength,effectiveStartIndex,effectiveEndIndex))
      }else if(endIndex > startIndexOfFile && endIndex < endIndexOfFile) {
        val effectiveStartIndex = math.max(startIndex-startIndexOfFile, 0)
        val effectiveEndIndex = endIndex-startIndexOfFile
        stringBuffer.append(Utils.offsetBytes(file.getAbsolutePath,fileLength,effectiveStartIndex,effectiveEndIndex))
      }
      sum += fileToLength(file)
      logDebug(s"After processing file $files, string built is ${stringBuffer.toString}")
    }
    stringBuffer.toString
  }

  def clone[T: ClassTag](value: T, serializer: SerializerInstance): T = {
    serializer.deserialize[T](serializer.serialize(value))
  }

  private def isSpace(c: Char): Boolean = {
    "\t\r\n".indexOf(c) != -1
  }

  def splitCommandString(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var inWord = false
    var inSingleQuote = false
    var inDoubleQuote = false
    val curWord = new StringBuilder
    def endWord(): Unit = {
      buf += curWord.toString
      curWord.clear()
    }
    var i = 0
    while(i < s.length) {
      val nextChar = s.charAt(i)
      if(inDoubleQuote) {
        if(nextChar == '"'){
          inDoubleQuote=false
        }else if (nextChar == '\\'){
          if (i<s.length-1){
            curWord.append(s.charAt(i+1))
            i += 1
          }
        }else {
          curWord.append(nextChar)
        }
      }else if(inSingleQuote) {
        if(nextChar == '\''){
          inSingleQuote=false
        }else{
          curWord.append(nextChar)
        }
      }else if (nextChar == '"'){
        inWord=true
        inDoubleQuote=true
      }else if(nextChar == '\''){
        inWord=true
        inSingleQuote=true
      }else if(!isSpace(nextChar)) {
        curWord.append(nextChar)
        inWord=true
      }else if(inWord && isSpace(nextChar)) {
        endWord()
        inWord=false
      }
      i += 1
    }
    if(inWord || inDoubleQuote || inSingleQuote) {
      endWord()
    }
    buf.toSeq
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x%mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def nonNegativeHash(obj: AnyRef): Int = {
    if(obj eq null) return 0
    val hash = obj.hashCode
    val hashAbs = if(Int.MinValue != hash) math.abs(hash) else 0
    hashAbs
  }

  def getSystemProperties: Map[String,String] = {
    System.getProperties.stringPropertyNames().asScala
      .map(key => (key,System.getProperty(key))).toMap
  }

  def times(numIters: Int)(f: => Unit): Unit = {
    var i = 0
    while (i<numIters) {
      f
      i += 1
    }
  }

  def timeIt(numIters: Int)(f: => Unit, prepare: Option[() => Unit] = None): Long = {
    if(prepare.isEmpty){
      val startNs = System.nanoTime()
      times(numIters)(f)
      System.nanoTime()-startNs
    }else {
      var i=0
      var sum = 0L
      while (i<numIters) {
        prepare.get.apply()
        val startNs = System.nanoTime()
        f
        sum += System.nanoTime() - startNs
        i += 1
      }
      sum
    }
  }

  def getIteratorSize(iterator: Iterator[_]): Long = Iterators.size(iterator)

  def getIteratorZipWithIndex[T](iter: Iterator[T], startIndex: Long): Iterator[(T,Long)] = {
    new Iterator[(T, Long)] {
      require(startIndex >=0, "startIndex should be >= 0.")
      var index: Long = startIndex - 1L
      def hasNext: Boolean = iter.hasNext
      def next(): (T, Long) = {
        index += 1L
        (iter.next(), index)
      }
    }
  }

  def symlink(src: File, dst: File): Unit = {
    if(!src.isAbsolute){
      throw new IOException("Source must be absolute")
    }
    if(dst.isAbsolute){
      throw new IOException("Destination must be relative")
    }
    Files.createSymbolicLink(dst.toPath, src.toPath)
  }

  def getFormattedClassName(obj: AnyRef): String = {
    getSimpleName(obj.getClass).replace("$","")
  }

  def getHadoopFileSystem(path: URI, conf: Configuration): FileSystem = {
    FileSystem.get(path, conf)
  }

  val isWindows = SystemUtils.IS_OS_WINDOWS
  val isMac = SystemUtils.IS_OS_MAC_OSX
  val isMacOnAppleSilicon = SystemUtils.IS_OS_MAC_OSX && SystemUtils.OS_ARCH.equals("aarch64")
  val preferIPv6 = "true".equals(System.getProperty("java.net.preferIPv6Addresses"))
  val windowsDrive = "([a-zA-Z])".r

  def isTesting: Boolean = {
    System.getenv("SPARK_TESTING") != null || System.getProperty(IS_TESTING.key) != null
  }

  def terminateProcess(process: Process, timeoutMs: Long): Option[Int] = {
    process.destroy()
    if (process.waitFor(timeoutMs,TimeUnit.MILLISECONDS)) {
      Option(process.exitValue())
    }else{
      try{
        process.destroyForcibly()
      }catch {
        case NonFatal(e) => logWarning("Exception when attempting to kill process",e)
      }
      if(process.waitFor(timeoutMs,TimeUnit.MILLISECONDS)) {
        Option(process.exitValue())
      }else{
        logWarning("Timed out waiting to forcibly kill process")
        None
      }
    }
  }

  def getStderr(process: Process, timeoutMs: Long): Option[String] = {
    val terminated = process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)
    if(terminated){
      Some(Source.fromInputStream(process.getErrorStream).getLines().mkString("\n"))
    }else{
      None
    }
  }

  def logUncaughtException[T](f: => T): T = {
    try{
      f
    }catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  def tryLog[T](f: => T): Try[T] = {
    try{
      val res = f
      scala.util.Success(res)
    }catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        scala.util.Failure(t)
    }
  }

  def isFatalError(e: Throwable): Boolean = {
    e match {
      case NonFatal(_) |
        _: InterruptedException |
        _: NotImplementedError |
        _: ControlThrowable |
        _: LinkageError =>
        false
      case _ =>
        true
    }
  }

  def resolveURI(path: String): URI = {
    try{
      val uri = new URI(path)
      if(uri.getScheme != null){
        return uri
      }
      if(uri.getFragment != null){
        val absoluteURI = new File(uri.getPath).getAbsoluteFile.toURI
        return new URI(absoluteURI.getScheme,absoluteURI.getHost,absoluteURI.getPath,uri.getFragment)
      }
    }catch {
      case e: URISyntaxException =>
    }
    new File(path).getCanonicalFile.toURI
  }

  def resolveURIs(paths: String): String = {
    if(paths == null || paths.trim.isEmpty) {
      ""
    }else{
      paths.split(",").filter(_.trim.nonEmpty).map{p => Utils.resolveURI(p)}.mkString(",")
    }
  }

  def isAbsoluteURI(path: String): Boolean = {
    try {
      val uri = new URI(path: String)
      uri.isAbsolute
    }catch {
      case _: URISyntaxException =>
        false
    }
  }

  def nonLocalPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    val windows = isWindows || testWindows
    if (paths == null || paths.trim.isEmpty){
      Array.empty
    }else{
      paths.split(",").filter{ p =>
        val uri = resolveURI(p)
        Option(uri.getScheme).getOrElse("file") match {
          case windowsDrive(d) if windows => false
          case "local" | "file" => false
          case _ => true
        }
      }
    }
  }

  def loadDefaultSparkProperties(conf: SparkConf, filePath: String = null): String = {

  }
}
