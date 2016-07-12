package tech.artemisia.task.database.mysql

import tech.artemisia.core.AppLogger
import tech.artemisia.task.database.{DBImporter, DBDataExporter, DBInterface}
import tech.artemisia.task.settings.{ExportSetting, LoadSetting}

/**
 * Created by chlr on 5/1/16.
 */

trait MySQLDataTransporter extends DBDataExporter with DBImporter {
  self: DBInterface =>

  override def load(tableName: String, loadSettings: LoadSetting) = {
    AppLogger debug "error file is ignored in this mode"
    this.execute(MySQLDataTransporter.getLoadSQL(tableName, loadSettings)) -> 0L
  }

  override def export(sql: String, exportSetting: ExportSetting) = {
    throw new UnsupportedOperationException("bulk export utility is not supported")
  }

}

object MySQLDataTransporter {

  def getLoadSQL(tableName: String, loadSettings: LoadSetting) = {
    s"""
       | LOAD DATA LOCAL INFILE '${loadSettings.location.getPath}'
       | INTO TABLE $tableName FIELDS TERMINATED BY '${loadSettings.delimiter}' ${if (loadSettings.quoting) s"OPTIONALLY ENCLOSED BY '${loadSettings.quotechar}'"  else ""}
       | ESCAPED BY '${if (loadSettings.escapechar == '\\') "\\\\" else loadSettings.escapechar }'
       | IGNORE ${loadSettings.skipRows} LINES
     """.stripMargin
  }

}
