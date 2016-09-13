package tech.artemisia.task.database.teradata.tdch

import com.typesafe.config.Config
import tech.artemisia.task.Task
import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 9/2/16.
  */
class TDCHLoadFromHive(override val taskName: String, val dBConnection: DBConnection, val sourceTable: String
                       , val targetTable: String, val method: String = "batch.insert", val truncate: Boolean = false
                       , val tdchHadoopSetting: TDCHSetting) extends Task(taskName) {


  override protected[task] def setup(): Unit = ???

  override protected[task] def work(): Config = ???

  override protected[task] def teardown(): Unit = ???

}
