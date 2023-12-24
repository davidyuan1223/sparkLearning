package org.apache.spark.internal.config

private [spark] object Tests {
  val TEST_USE_COMPRESSED_OOPS_KEY = "spark.test.useCompressedOops"

  val TEST_MEMORY = ConfigBuilder("spark.testing.memory")
    .version("1.6.0")
    .longConf
    .createWithDefault(Runtime.getRuntime.maxMemory)

  val TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED=
    ConfigBuilder("spark.testing.dynamicAllocation.schedule.enabled")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val IS_TESTING = ConfigBuilder("spark.testing")
    .version("1.0.1")
    .booleanConf
    .createOptional

  val TEST_NO_STAGE_RETRY = ConfigBuilder("spark.test.noStageRetry")
    .version("1.2.0")
    .booleanConf
    .createWithDefault(false)

  val TEST_RESERVED_MEMORY = ConfigBuilder("spark.testing.reservedMemory")
    .version("1.6.0")
    .longConf
    .createOptional

  val TEST_N_HOSTS = ConfigBuilder("spark.testing.nHosts")
    .version("3.0.0")
    .intConf
    .createWithDefault(5)

  val TEST_N_EXECUTORS_HOST = ConfigBuilder("spark.testing.nExecutorsPerHost")
    .version("3.0.0")
    .intConf
    .createWithDefault(4)

  val TEST_N_CORES_EXECUTOR = ConfigBuilder("spark.testing.nCoresPerExecutor")
    .version("3.0.0")
    .intConf
    .createWithDefault(2)

  val RESOURCES_WARNING_TESTING = ConfigBuilder("spark.resources.warnings.testing")
    .version("3.1.0")
    .booleanConf
    .createWithDefault(false)

  val RESOURCE_PROFILE_MANAGER_TESTING = ConfigBuilder("spark.testing.resourceProfileManager")
    .version("3.1.0")
    .booleanConf
    .createWithDefault(false)

  val SKIP_VALIDATE_CORES_TESTING = ConfigBuilder("spark.testing.skipValidateCores")
    .version("3.1.0")
    .booleanConf
    .createWithDefault(false)
}
