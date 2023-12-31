package org.apache.spark.internal.config

import java.util.concurrent.TimeUnit

private [spark] object Network {
  private[spark] val NETWORK_CRYPTO_SASL_FALLBACK =
    ConfigBuilder("spark.network.crypto.saslFallback")
      .version("2.2.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val NETWORK_CRYPTO_ENABLED =
    ConfigBuilder("spark.network.crypto.enabled")
      .version("2.2.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val NETWORK_REMOTE_READ_NIO_BUFFER_CONVERSION =
    ConfigBuilder("spark.network.remoteReadNioBufferConversion")
      .version("2.4.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val NETWORK_TIMEOUT =
    ConfigBuilder("spark.network.timeout")
      .version("1.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("120s")

  private[spark] val NETWORK_TIMEOUT_INTERVAL =
    ConfigBuilder("spark.network.timeoutInterval")
      .version("1.3.2")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString(STORAGE_BLOCKMANAGER_TIMEOUTINTERVAL.defaultValueString)

  private[spark] val RPC_ASK_TIMEOUT =
    ConfigBuilder("spark.rpc.askTimeout")
      .version("1.4.0")
      .stringConf
      .createOptional

  private[spark] val RPC_CONNECT_THREADS =
    ConfigBuilder("spark.rpc.connect.threads")
      .version("1.6.0")
      .intConf
      .createWithDefault(64)

  private[spark] val RPC_IO_NUM_CONNECTIONS_PER_PEER =
    ConfigBuilder("spark.rpc.io.numConnectionsPerPeer")
      .version("1.6.0")
      .intConf
      .createWithDefault(1)

  private[spark] val RPC_IO_THREADS =
    ConfigBuilder("spark.rpc.io.threads")
      .version("1.6.0")
      .intConf
      .createOptional

  private[spark] val RPC_LOOKUP_TIMEOUT =
    ConfigBuilder("spark.rpc.lookupTimeout")
      .version("1.4.0")
      .stringConf
      .createOptional

  private[spark] val RPC_MESSAGE_MAX_SIZE =
    ConfigBuilder("spark.rpc.message.maxSize")
      .version("2.0.0")
      .intConf
      .createWithDefault(128)

  private[spark] val RPC_NETTY_DISPATCHER_NUM_THREADS =
    ConfigBuilder("spark.rpc.netty.dispatcher.numThreads")
      .version("1.6.0")
      .intConf
      .createOptional
}
