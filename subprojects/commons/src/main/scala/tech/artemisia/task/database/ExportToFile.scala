package tech.artemisia.task.database

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.Task
import tech.artemisia.task.settings.{ConnectionProfile, ExportSetting}

/**
 * Created by chlr on 4/13/16.
 */

/**
  *
  * @param name name of the task instance
  * @param sql query for the export
  * @param connectionProfile Connection Profile settings
  * @param exportSettings Export settings
  */
abstract class ExportToFile(name: String, sql: String, connectionProfile: ConnectionProfile ,exportSettings: ExportSetting)
  extends Task(name: String) {

     val dbInterface: DBInterface

     override protected[task] def setup(): Unit = {}

     /**
      *
      * SQL export to file
      * @return Config object with key rows and values as total number of rows exports
      */
     override protected[task] def work(): Config = {
       val rs = dbInterface.query(sql)
       val records = DBUtil.exportCursorToFile(rs,exportSettings)
       wrapAsStats {
         ConfigFactory parseString
           s"""
             | rows = $records
           """.stripMargin
       }
     }

     override protected[task] def teardown(): Unit = {}

}

object ExportToFile {

  /**
    * @return one line description of the task
    */
  def info = "export query results to a file"

}



