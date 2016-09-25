package tech.artemisia.task.database.teradata.tpt

import com.typesafe.config.Config

/**
  * Created by chlr on 9/23/16.
  */

trait BaseTPTLogParser {

  var jobId: String

  var jobLogFile: String

  def toConfig: Config

}
