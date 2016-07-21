package tech.artemisia.task.database.mysql

import java.io.{InputStream, OutputStream}
import java.net.URI

import tech.artemisia.core.AppLogger
import tech.artemisia.task.database.{DBExporter, DBImporter, DBInterface}
import tech.artemisia.task.settings.{ExportSetting, LoadSetting}


/**
 * Created by chlr on 5/1/16.
 */

trait MySQLDataTransporter extends DBExporter with DBImporter {

  self: DBInterface =>

  override def load(tableName: String, location: URI, loadSettings: LoadSetting) = {
    AppLogger debug "error file is ignored in this mode"
    this.execute(MySQLDataTransporter.getLoadSQL(tableName, location, loadSettings)) -> 0L
  }

  override def load(sql: String, inputStream: InputStream, loadSetting: LoadSetting) = {
    throw new UnsupportedOperationException("bulk load utility is not supported")
  }

  override def export(sql: String, outputStream: OutputStream, exportSetting: ExportSetting) = {
    throw new UnsupportedOperationException("bulk export utility is not supported")
  }

  override def export(sql: String, location: URI, exportSetting: ExportSetting) = {
    throw new UnsupportedOperationException("bulk export utility is not supported")
  }
}

object MySQLDataTransporter {

  def getLoadSQL(tableName: String, location: URI ,loadSettings: LoadSetting) = {
    s"""
       | LOAD DATA LOCAL INFILE '${location.getPath}'
       | INTO TABLE $tableName FIELDS TERMINATED BY '${loadSettings.delimiter}' ${if (loadSettings.quoting) s"OPTIONALLY ENCLOSED BY '${loadSettings.quotechar}'"  else ""}
       | ESCAPED BY '${if (loadSettings.escapechar == '\\') "\\\\" else loadSettings.escapechar }'
       | IGNORE ${loadSettings.skipRows} LINES
     """.stripMargin
  }

}
