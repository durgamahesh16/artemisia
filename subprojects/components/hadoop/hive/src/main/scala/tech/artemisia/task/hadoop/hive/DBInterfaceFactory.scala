package tech.artemisia.task.hadoop.hive

import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 8/1/16.
  */

object DBInterfaceFactory {


  def getDBInterface(connectionProfile: DBConnection) = {
    connectionProfile match {
      case DBConnection(null, null, null, null, -1) => new HiveCLIDBInterface
      case profile => new HiveServerDBInterface(profile)
    }
  }


}
