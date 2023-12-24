package org.apache.spark.internal

import org.apache.logging.log4j.core.appender.ConsoleAppender
import org.apache.logging.log4j.core.config.DefaultConfiguration
import org.apache.logging.log4j.core.filter.AbstractFilter
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{Filter, LifeCycle, LogEvent, LoggerContext, Logger => Log4jLogger}
import org.apache.spark.internal.Logging.SparkShellLoggingFilter
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.mapAsScalaMapConverter

trait Logging {
  @transient private var log_ : Logger = null
  protected def logName = this.getClass.getName.stripSuffix("$")
  protected def log: Logger = {
    if (log_ == null) {
      initializeLogIfNecessary(false)
      log_  = LoggerFactory.getLogger(logName)
    }
    log_
  }
  // Log methods that take only a String
  protected def logInfo(msg: => String): Unit = {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String): Unit = {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String): Unit = {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String): Unit = {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String): Unit = {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable): Unit = {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable): Unit = {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable): Unit = {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable): Unit = {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  protected def initializeLogIfNecessary(isInterpreter: Boolean): Unit = {
    initializeLogIfNecessary(isInterpreter, silent = false)
  }
  protected def initializeLogIfNecessary(isInterpreter: Boolean, silent: Boolean = false): Boolean = {
    if (!Logging.initialized) {
      Logging.initLock.synchronized{
        if (!Logging.initialized){
          initializeLogging(isInterpreter,silent)
          return true
        }
      }
    }
    false
  }
  private [spark] def initializeForcefully(isInterpreter: Boolean, silent: Boolean): Unit = {
    initializeLogging(isInterpreter, silent)
  }
  private def initializeLogging(isInterpreter: Boolean, silent: Boolean): Unit = {
    if (Logging.isLog4j2()) {
      val rootLogger = LogManager.getRootLogger.asInstanceOf[Log4jLogger]
      if (Logging.islog4j2DefaultConfigured()){
        Logging.defaultSparkLog4jConfig = true
        val defaultLogProps = "org/apache/spark/log4j2-defaults.properties"
        Option(Utils.getSparkClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
            context.setConfiguration(url.toURI)
            if (!silent) {
              System.err.println(s"Using Spark's default log4j profile: $defaultLogProps")
            }
          case None =>
            System.err.println(s"Spark was unable to load $defaultLogProps")
        }
      }

      if (Logging.defaultRootLevel == null) {
        Logging.defaultRootLevel = rootLogger.getLevel
      }

      if (isInterpreter) {
        val replLogger = LogManager.getLogger(logName).asInstanceOf[Log4jLogger]
        val replLevel = if (Logging.loggerWithCustomConfig(replLogger)) {
          replLogger.getLevel
        }else {
          Level.WARN
        }
        if (replLevel != rootLogger.getLevel) {
          if (!silent) {
            System.err.printf("Setting default log level to \"%s\".\n",replLevel)
            System.err.println("To adjust logging level use sc.setLogLevel(newLevel). " +
              "For SparkR, use setLogLevel(newLevel).")
          }
          Logging.sparkShellThresholdLevel = replLevel
          rootLogger.getAppenders.asScala.foreach {
            case (_, ca: ConsoleAppender) =>
              ca.addFilter(new SparkShellLoggingFilter())
            case _ =>
          }
        }
      }
    }
    Logging.initialized=true
    log
  }
}

private [spark] object Logging {
  @volatile private var initialized = false
  @volatile private var defaultRootLevel: Level = null
  @volatile private var defaultSparkLog4jConfig = false
  @volatile private [spark] var sparkShellThresholdLevel: Level = null

  val initLock = new Object
  try{
    val bridgeClass = Utils.classForName("org.slf4j.bridge.SLF4jBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed){
      bridgeClass.getMethod("install").invoke(null)
    }
  }catch {
    case e: ClassNotFoundException =>
  }

  def uniniitialize(): Unit = initLock.synchronized{
    if (isLog4j2()) {
      if (defaultSparkLog4jConfig) {
        defaultSparkLog4jConfig=false
        val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
        context.reconfigure()
      }else {
        val rootLogger = LogManager.getRootLogger.asInstanceOf[Log4jLogger]
        rootLogger.setLevel(defaultRootLevel)
        sparkShellThresholdLevel=null
      }
    }
    this.initialized=false
  }

  private def isLog4j2(): Boolean = {
    "org.apche.logging.slf4j.Log4jLoggerFactory"
      .equals(LoggerFactory.getILoggerFactory.getClass.getName)
  }

  private def loggerWithCustomConfig(logger: Log4jLogger): Boolean = {
    val rootConfig = LogManager.getRootLogger.asInstanceOf[Log4jLogger].get()
    (logger.get() ne rootConfig) || (logger.getLevel != rootConfig.getLevel)
  }

  private [spark] def islog4j2DefaultConfigured(): Boolean = {
    val rootLogger = LogManager.getRootLogger.asInstanceOf[Log4jLogger]
    rootLogger.getAppenders.isEmpty ||
      (rootLogger.getAppenders.size() == 1 &&
        rootLogger.getLevel == Level.ERROR &&
        LogManager.getContext.asInstanceOf[LoggerContext]
        .getConfiguration.isInstanceOf[DefaultConfiguration])
  }

  private [spark] class SparkShellLoggingFilter extends AbstractFilter {
    private var status = LifeCycle.State.INITIALIZING

    override def filter(event: LogEvent): Filter.Result = {
      if (Logging.sparkShellThresholdLevel == null) {
        Filter.Result.NEUTRAL
      }else if( event.getLevel.isMoreSpecificThan(Logging.sparkShellThresholdLevel)) {
        Filter.Result.NEUTRAL
      }else {
        val logger = LogManager.getLogger(event.getLoggerName).asInstanceOf[Log4jLogger]
        if (loggerWithCustomConfig(logger)) {
          return Filter.Result.NEUTRAL
        }
        Filter.Result.DENY
      }
    }

    override def getState: LifeCycle.State = status

    override def initialize(): Unit = {
      status = LifeCycle.State.INITIALIZED
    }

    override def start(): Unit = {
      status = LifeCycle.State.STARTED
    }

    override def stop(): Unit = {
      status = LifeCycle.State.STOPPED
    }

    override def isStarted: Boolean = status == LifeCycle.State.STARTED

    override def isStopped: Boolean = status == LifeCycle.State.STOPPED
  }
}
