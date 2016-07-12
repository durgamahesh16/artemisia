package tech.artemisia.task.database.postgres

import java.io._
import org.postgresql.PGConnection
import tech.artemisia.core.AppLogger
import tech.artemisia.task.database.{DBImporter, DBDataExporter, DBInterface}
import tech.artemisia.task.settings.{ExportSetting, LoadSetting}
import tech.artemisia.util.Util

/**
  * Created by chlr on 6/11/16.
  */

trait PGDataTransporter extends DBDataExporter with DBImporter {

  self: DBInterface =>

  override def load(tableName: String, loadSettings: LoadSetting) = {
    val copyMgr = self.connection.asInstanceOf[PGConnection].getCopyAPI
    val reader = new BufferedReader(new FileReader(new File(loadSettings.location)))
    AppLogger info Util.prettyPrintAsciiBanner(PGDataTransporter.getLoadCmd(tableName, loadSettings), heading = "query")
     val result = copyMgr.copyIn(PGDataTransporter.getLoadCmd(tableName, loadSettings), reader)
    reader.close()
    result -> -1L
  }

  override def export(sql: String, exportSetting: ExportSetting) = {
    val copyMgr = self.connection.asInstanceOf[PGConnection].getCopyAPI
    val writer = new BufferedWriter(new FileWriter(new File(exportSetting.file)))
    AppLogger info Util.prettyPrintAsciiBanner(PGDataTransporter.getExportCmd(sql, exportSetting), heading = "query")
    val rowCount = copyMgr.copyOut(PGDataTransporter.getExportCmd(sql, exportSetting), writer)
    writer.close()
    rowCount
  }

}

object PGDataTransporter {

  private[postgres] def getLoadCmd(tableName: String, loadSettings: LoadSetting) = {
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
