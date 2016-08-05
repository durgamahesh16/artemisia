package tech.artemisia.task.hadoop.hive

import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 8/1/16.
  */

object DBInterfaceFactory {

  def getDBInterface(connectionProfile: Option[DBConnection]) = {
    connectionProfile match {
      case None => new HiveCLIInterface
      case Some(profile) => new HiveServerDBInterface(profile)
    }
  }

}
