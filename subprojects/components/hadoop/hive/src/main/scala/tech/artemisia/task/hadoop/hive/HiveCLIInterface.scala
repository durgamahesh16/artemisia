package tech.artemisia.task.hadoop.hive

import java.io.{InputStream, OutputStream}
import java.net.URI
import java.sql.Connection
import tech.artemisia.util.FileSystemUtil._
import tech.artemisia.task.TaskContext
import tech.artemisia.task.database.{DBExporter, DBImporter, DBInterface}
import tech.artemisia.task.settings.{ExportSetting, LoadSetting}
import tech.artemisia.util.CommandUtil._

/**
  * Created by chlr on 8/3/16.
  */
class HiveCLIInterface extends DBInterface with DBImporter with DBExporter {

  /**
    * This method is unimplemented and will raise an scala.NotImplementedError if accesssed.
    *
    * @return JDBC connection object
    */
  def getNewConnection: Connection = ???
  // we trade compile time safety with


  def execute(sql: String): Long = {
    val cmd = makeHiveCommand(sql)
    val retCode = executeCmd(cmd)
    assert(retCode == 0, s"query execution failed with ret code $retCode")
    0L
  }

  def makeHiveCommand(sql: String): Seq[String] = {
    val file = TaskContext.getTaskFile("query.hql")
    file <<= sql
    getExecutablePath("hive") match {
      case Some(exe) => exe :: "-f" :: file.toPath.toString :: Nil
      case None => throw new RuntimeException("hive command not found")
    }
  }

  override def load(tableName: String, inputStream: InputStream, loadSetting: LoadSetting): (Long, Long) = ???

  override def load(tableName: String, location: URI, loadSetting: LoadSetting): (Long, Long) = ???

  override def export(sql: String, outputStream: OutputStream, exportSetting: ExportSetting): Long = ???

  override def export(sql: String, location: URI, exportSetting: ExportSetting): Long = ???

}
