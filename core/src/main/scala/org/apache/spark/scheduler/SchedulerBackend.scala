package org.apache.spark.scheduler

private [spark] trait SchedulerBackend {
  private val appId = "spark-application-"+System.currentTimeMillis()
  def start(): Unit
  def stop(): Unit
  def stop(exitCode: Int): Unit = stop()
  def reviveOffers(): Unit
  def defaultParallelism(): Int
  def killTask(taskId: Long, executorId: String,
               interruptThread: Boolean, reason: String): Unit =
    throw new UnsupportedOperationException
  def isReady(): Boolean = true
  def applicationId(): String = appId
  def applicationAttemptId(): Option[String] = None
  def getDriverLogUrls: Option[Map[String,String]] = None
  def getDriverAttributes: Option[Map[String,String]] = None
  def maxNumConcurrentTasks(rp: ResourceProfile): Int
  def getShufflePushMergerLocations(numPartitions: Int,
                                    resourceProfileId: Int): Seq[BlockManagerId] = Nil

}
