package tech.artemisia.task.hadoop.hive

import java.io.{InputStream, OutputStream}
import java.net.URI
import java.sql.Connection

import tech.artemisia.task.database.{DBExporter, DBImporter, DBInterface}
import tech.artemisia.task.settings.{ExportSetting, LoadSetting}

/**
  * Created by chlr on 8/1/16.
  */
class HiveCLIDBInterface extends DBInterface with DBImporter with DBExporter {


  override def getNewConnection: Connection = ???

  override def load(tableName: String, inputStream: InputStream, loadSetting: LoadSetting): (Long, Long) = ???

  override def load(tableName: String, location: URI, loadSetting: LoadSetting): (Long, Long) = ???

  override def export(sql: String, outputStream: OutputStream, exportSetting: ExportSetting): Long = ???

  override def export(sql: String, location: URI, exportSetting: ExportSetting): Long = ???

}
