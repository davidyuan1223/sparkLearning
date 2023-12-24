package org.apache.spark.scheduler

import org.apache.spark.annotation.DeveloperApi

@DeveloperApi
class StageInfo(val stageId: Int,
                private val attemptId: Int,
                val name: String,
                val numTasks: Int,
                val rddInfos: Seq[RDDInfo],
                val parentIds: Seq[Int],
                val details: String,
                val taskMetrics: TaskMetrics=null,
                private [spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,
                private [spark] val shuffleDepId: Option[Int] = None,
                val resourceProfileId: Int,
                private [spark] var isShufflePushEnabled: Boolean=false,
                private [spark] var shuffleMergerCount: Int = 0) {

}
