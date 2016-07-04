package tech.artemisia.task.database.teradata

import java.sql.SQLException
import tech.artemisia.task.database.{BaseDBWriter, DBInterface}
import tech.artemisia.task.settings.LoadSetting
import scala.collection.JavaConverters._

/**
  * This DBWriter instance differs from the default DBWriter on how BatchUpdateException is handled
  *
  * @param tableName    name of the table
  * @param loadSettings load settings
  * @param dBInterface  database interface object
  */
class FastLoadDBWriter(tableName: String, loadSettings: LoadSetting, dBInterface: DBInterface)
  extends BaseDBWriter(tableName, loadSettings, dBInterface) {

  def processBatch(batch: Array[Array[String]]) = ???

  override def processRow(row: Array[String]): Unit = {
    try {
      composeStmt(row)
      stmt.addBatch()
    } catch {
      case th: SQLException =>
        errorWriter.writeRow(row :+ th.iterator.asScala.toList.last.getMessage)
    }
  }

  private def getErrorMessage(th: Throwable) = {
    val rgx = """[.*] [.*] [.*] (.*)""".r
  }
}
