package tech.artemisia.task.database.postgres

import java.io._

import org.postgresql.PGConnection
import tech.artemisia.core.AppLogger
import tech.artemisia.task.database.{DBInterface, DataTransporter}
import tech.artemisia.task.settings.{ExportSetting, LoadSettings}
import tech.artemisia.util.Util

/**
  * Created by chlr on 6/11/16.
  */

trait PostgresFileTransporter extends DataTransporter {

  self: DBInterface =>

  override def loadData(tableName: String, loadSettings: LoadSettings) = {
    val copyMgr = self.connection.asInstanceOf[PGConnection].getCopyAPI
    val reader = new BufferedReader(new FileReader(new File(loadSettings.location)))
    AppLogger info Util.prettyPrintAsciiTable(getLoadCmd(tableName, loadSettings), heading = "query")
     val result = copyMgr.copyIn(getLoadCmd(tableName, loadSettings), reader)
    reader.close()
    result -> -1L
  }

  override def exportData(sql: String, exportSetting: ExportSetting) = {
    val copyMgr = self.connection.asInstanceOf[PGConnection].getCopyAPI
    val writer = new BufferedWriter(new FileWriter(new File(exportSetting.file)))
    AppLogger info Util.prettyPrintAsciiTable(getExportCmd(sql, exportSetting), heading = "query")
    val rowCount = copyMgr.copyOut(getExportCmd(sql, exportSetting), writer)
    writer.close()
    rowCount
  }

  private[postgres] def getLoadCmd(tableName: String, loadSettings: LoadSettings) = {
    assert(loadSettings.skipRows <= 1, "this task can skip either 0 or 1 row only")
   s"""
      | COPY $tableName FROM STDIN
      | WITH (DELIMITER '${loadSettings.delimiter}',
      | FORMAT csv,
      | ${if (loadSettings.quoting) s"QUOTE '${loadSettings.quotechar}'" },
      | ESCAPE '${loadSettings.escapechar}',
      | HEADER ${if (loadSettings.skipRows == 1) "ON" else "OFF"}
      | )
    """.stripMargin
  }

  private[postgres] def getExportCmd(sql: String, exportSetting: ExportSetting) = {
      s"""
         |COPY ($sql) TO STDOUT
         |WITH (DELIMITER '${exportSetting.delimiter}',
         |FORMAT csv,
         |FORCE_QUOTE *,
         |${if (exportSetting.quoting) s"QUOTE '${exportSetting.quotechar}'" },
         |ESCAPE '${exportSetting.escapechar}',
         |HEADER ${if (exportSetting.header) "ON" else "OFF"}
         |)
       """.stripMargin
  }

}
