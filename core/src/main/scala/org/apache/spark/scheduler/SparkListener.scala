package org.apache.spark.scheduler

import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.annotation.{DeveloperApi, Since}

import java.util.Properties
import javax.annotation.Nullable

@DeveloperApi
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
trait SparkListenerEvent {
  protected[spark] def logEvent: Boolean = true
}

@DeveloperApi
case class SparkListenerStageSubmitted(stageInfo: StageInfo, properties: Properties = null) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskStart(stageId: Int, stageAttemptId: Int, taskInfo: TaskInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskGettingResult(taskInfo: TaskInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerSpeculativeTaskSubmitted(stageId: Int, stageAttemptId: Int = 0) extends SparkListenerEvent {
  private var _taskIndex: Int = -1
  private var _partitionId: Int = -1

  def taskIndex: Int = _taskIndex

  def partitionId: Int = _partitionId

  def this(stageId: Int, stageAttemptId: Int, taskIndex: Int, partitionId: Int) = {
    this(stageId, stageAttemptId)
    _partitionId = partitionId
    _taskIndex = taskIndex
  }
}

@DeveloperApi
case class SparkListenerTaskEnd(stageId: Int, stageAttemptId: Int,
                                taskType: String, reason: TaskEndReason,
                                taskInfo: TaskInfo, taskExecutorMetrics: ExecutorMetrics,
                                @Nullable taskMetrics: TaskMetrics) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerJobStart(jobId: Int, time: Long,
                                 stageInfos: Seq[StageInfo], properties: Properties = null) extends SparkListenerEvent {
  val stageIds: Seq[Int] = stageInfos.map(_.stageId)
}

@DeveloperApi
case class SparkListenerJobEnd(jobId: Int, time: Long, jobResult: JobResult) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerEnvironmentUpdate(environmentDetails: Map[String, collection.Seq[(String, String)]]) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockManagerAdded(time: Long, blockManagerId: BlockManagerId,
                                        maxMem: Long, maxOnHeapMem: Option[Long] = None,
                                        maxOffHeapMam: Option[Long] = None) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockManagerRemoved(time: Long, blockManagerId: BlockManagerId) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerUnpersistRDD(rddId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorAdded(time: Long, executorId: String, executorInfo: ExecutorInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorRemoved(time: Long, executorId: String, reason: String) extends SparkListenerEvent

@DeveloperApi
@deprecated("use SparkListenerExecutorExcluded instead", "3.1.0")
case class SparkListenerExecutorBlacklisted(time: Long, executorId: String, taskFailures: Int) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcluded(time: Long, executorId: String, taskFailures: Int) extends SparkListenerEvent

@DeveloperApi
@deprecated("use SparkListenerExecutorExcludedForStage instead", "3.1.0")
case class SparkListenerExecutorBlacklistedForStage(time: Long, executor: String, taskFailures: Int,
                                                    stageId: Int, stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcludedForStage(time: Long, executor: String, taskFailures: Int,
                                                 stageId: Int, stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
@deprecated("use SparkListenerNodeExcludedForStage instead", "3.1.0")
case class SparkListenerNodeBlacklistedForStage(time: Long, hostId: String, taskFailures: Int,
                                                stageId: Int, stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeExcludedForStage(time: Long, hostId: String, taskFailures: Int,
                                             stageId: Int, stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
@deprecated("use SparkListenerExecutorUnexcluded insteand", "3.1.0")
case class SparkListenerExecutorUnblacklisted(time: Long, executorId: String) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorUnexcluded(time: Long, executorId: String) extends SparkListenerEvent

@DeveloperApi
@deprecated("use SparkListenerNodeExcluded instead", "3.1.0")
case class SparkListenerNodeBlacklisted(time: Long, hostId: String, taskFailures: Int) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeExcluded(time: Long, hostId: String, taskFailures: Int) extends SparkListenerEvent

@DeveloperApi
@deprecated("use SparkListenerNodeUnExcluded instead", "3.1.0")
case class SparkListenerNodeUnblacklisted(time: Long, hostId: String) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeUnexcluded(time: Long, hostId: String) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetAdded(stageId: Int, stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetRemoved(stageId: Int, stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockUpdated(blockUpdatedInfo: BlockUpdatedInfo) extends SparkListenerEvent

@DeveloperApi
@Since("3.2.0")
case class SparkListenerMiscellaneousProcessAdded(time: Long, processId: String,
                                                  info: MiscellaneousProcessDetails) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorMetricsUpdate(execId: String,
                                              accumUpdates: Seq[(Long, Int, Int, Seq[AccumulableInfo])],
                                              executorUpdates: Map[(Int, Int), ExecutorMetrics] = Map.empty) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerStageExecutorMetrics(execId: String,
                                             accumUpdates: Seq[(Long, Int, Int, Seq[AccumulableInfo])],
                                             executorUpdates: Map[(Int, Int), ExecutorMetrics] = Map.empty) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationStart(appName: String, appId: Option[String],
                                         time: Long, sparkUser: String, appAttemptId: Option[String],
                                         driverLogs: Option[Map[String, String]] = None,
                                         driverAttributes: Option[Map[String, String]] = None) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationEnd(time: Long) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerLogStart(sparkVersion: String) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerResourceProfileAdded(resourceProfile: ResourceProfile) extends SparkListenerEvent

private[spark] trait SparkListenerInterface {
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit

  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit

  def onTaskStart(taskStart: SparkListenerTaskStart): Unit

  def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit

  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit

  def onJobStart(jobStart: SparkListenerJobStart): Unit

  def onJobEnd(jobEnd: SparkListenerJobEnd): Unit

  def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit

  def onBlockManagerAdded(blockManagerAdd: SparkListenerBlockManagerAdded): Unit

  def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit

  def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit

  def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit

  def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit

  def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit

  def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit

  def onExecutorAdded(executorAdd: SparkListenerExecutorAdded): Unit

  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit

  @deprecated("use onExecutorExcluded instead", "3.1.0")
  def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit

  def onExecutorExcluded(executorExcluded: SparkListenerExecutorExcluded): Unit

  @deprecated("use onExecutorExcludedForStage instead", "3.1.0")
  def onExecutorBlacklistedForStage(executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit

  def onExecutorExcludedForStage(
                                  executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit

  /**
   * Called when the driver excludes a node for a stage.
   */
  @deprecated("use onNodeExcludedForStage instead", "3.1.0")
  def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit

  /**
   * Called when the driver excludes a node for a stage.
   */
  def onNodeExcludedForStage(nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit

  /**
   * Called when the driver re-enables a previously excluded executor.
   */
  @deprecated("use onExecutorUnexcluded instead", "3.1.0")
  def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit

  /**
   * Called when the driver re-enables a previously excluded executor.
   */
  def onExecutorUnexcluded(executorUnexcluded: SparkListenerExecutorUnexcluded): Unit

  /**
   * Called when the driver excludes a node for a Spark application.
   */
  @deprecated("use onNodeExcluded instead", "3.1.0")
  def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit

  /**
   * Called when the driver excludes a node for a Spark application.
   */
  def onNodeExcluded(nodeExcluded: SparkListenerNodeExcluded): Unit

  /**
   * Called when the driver re-enables a previously excluded node.
   */
  @deprecated("use onNodeUnexcluded instead", "3.1.0")
  def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit

  /**
   * Called when the driver re-enables a previously excluded node.
   */
  def onNodeUnexcluded(nodeUnexcluded: SparkListenerNodeUnexcluded): Unit

  /**
   * Called when a taskset becomes unschedulable due to exludeOnFailure and dynamic allocation
   * is enabled.
   */
  def onUnschedulableTaskSetAdded(
                                   unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit

  /**
   * Called when an unschedulable taskset becomes schedulable and dynamic allocation
   * is enabled.
   */
  def onUnschedulableTaskSetRemoved(
                                     unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit

  /**
   * Called when the driver receives a block update info.
   */
  def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit

  /**
   * Called when a speculative task is submitted
   */
  def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit

  /**
   * Called when other events like SQL-specific events are posted.
   */
  def onOtherEvent(event: SparkListenerEvent): Unit

  /**
   * Called when a Resource Profile is added to the manager.
   */
  def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit
}

@DeveloperApi
abstract class SparkListener extends SparkListenerInterface {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {}

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {}

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {}

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {}

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {}

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {}

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {}

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {}

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {}

  override def onBlockManagerRemoved(
                                      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {}

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {}

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {}

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {}

  override def onExecutorMetricsUpdate(
                                        executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {}

  override def onStageExecutorMetrics(
                                       executorMetrics: SparkListenerStageExecutorMetrics): Unit = {}

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {}

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {}

  override def onExecutorBlacklisted(
                                      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = {}

  override def onExecutorExcluded(
                                   executorExcluded: SparkListenerExecutorExcluded): Unit = {}

  override def onExecutorBlacklistedForStage(
                                              executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = {}

  override def onExecutorExcludedForStage(
                                           executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit = {}

  override def onNodeBlacklistedForStage(
                                          nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = {}

  override def onNodeExcludedForStage(
                                       nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit = {}

  override def onExecutorUnblacklisted(
                                        executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {}

  override def onExecutorUnexcluded(
                                     executorUnexcluded: SparkListenerExecutorUnexcluded): Unit = {}

  override def onNodeBlacklisted(
                                  nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = {}

  override def onNodeExcluded(
                               nodeExcluded: SparkListenerNodeExcluded): Unit = {}

  override def onNodeUnblacklisted(
                                    nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = {}

  override def onNodeUnexcluded(
                                 nodeUnexcluded: SparkListenerNodeUnexcluded): Unit = {}

  override def onUnschedulableTaskSetAdded(
                                            unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit = {}

  override def onUnschedulableTaskSetRemoved(
                                              unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit = {}

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {}

  override def onSpeculativeTaskSubmitted(
                                           speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = {}

  override def onOtherEvent(event: SparkListenerEvent): Unit = {}

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = {}
}

