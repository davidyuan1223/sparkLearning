package org.apache.spark.internal.config

import org.apache.spark.network.util.ByteUnit

private [spark] object Kryo {
  val KRYO_REGISTRATION_REQUIRED = ConfigBuilder("spark.kryo.registrationRequired")
    .version("1.1.0")
    .booleanConf
    .createWithDefault(false)

  val KRYO_USER_REGISTRATORS = ConfigBuilder("spark.kryo.registrator")
    .version("0.5.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val KRYO_CLASSES_TO_REGISTER = ConfigBuilder("spark.kryo.classesToRegister")
    .version("1.2.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val KRYO_USE_UNSAFE = ConfigBuilder("spark.kryo.unsafe")
    .version("2.1.0")
    .booleanConf
    .createWithDefault(true)

  val KRYO_USE_POOL = ConfigBuilder("spark.kryo.pool")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(true)

  val KRYO_REFERENCE_TRACKING = ConfigBuilder("spark.kryo.referenceTracking")
    .version("0.8.0")
    .booleanConf
    .createWithDefault(true)

  val KRYO_SERIALIZER_BUFFER_SIZE = ConfigBuilder("spark.kryoserializer.buffer")
    .version("1.4.0")
    .bytesConf(ByteUnit.KiB)
    .createWithDefaultString("64k")

  val KRYO_SERIALIZER_MAX_BUFFER_SIZE = ConfigBuilder("spark.kryoserializer.buffer.max")
    .version("1.4.0")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("64m")
}
