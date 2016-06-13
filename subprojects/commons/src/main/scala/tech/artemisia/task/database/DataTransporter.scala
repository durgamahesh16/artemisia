package tech.artemisia.task.database

import tech.artemisia.task.settings.{ExportSetting, LoadSettings}

/**
  * Created by chlr on 6/12/16.
  */

/**
  *
  */
trait DataTransporter {

  self: DBInterface =>

  /**
    * A generic function that loads a file to table by iterating each row of the file
    * and running INSERT INTO TABLE query
    *
    * @param tableName target table to load
    * @param loadSettings load settings
    * @return number of records inserted
    */
  def loadData(tableName: String, loadSettings: LoadSettings): (Long, Long)


  /**
    * export query to file
    * @param sql query
    * @param exportSetting export settings
    * @return no of records exported
    */
  def exportData(sql: String, exportSetting: ExportSetting): Long

}
