  package tech.artemisia.task.database.mysql

import java.nio.file.Paths
import java.sql.Connection

import tech.artemisia.TestSpec
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.{BasicExportSetting, BasicLoadSetting}

/**
 * Created by chlr on 6/14/16.
 */
class MySQLDataTransporterSpec extends TestSpec {

  "MYSQLDataTransporterSpec" must "throw unsupported exception for mysql bulk export" in {
    val sql = "select * from table"
    val exportSetting = BasicExportSetting(file = Paths.get("dummy_file").toUri)
    val transporter = new DBInterface with MySQLDataTransporter {
      override def getNewConnection: Connection = ???
    }
    val ex = intercept[UnsupportedOperationException]{
      transporter.export(sql, exportSetting)
    }
    ex.getMessage must be ("bulk export utility is not supported")
  }

  it must "generate load command" in {
    val loadSetting = BasicLoadSetting(location = Paths.get("dummy_file").toUri, skipRows = 1, delimiter = '\u0001')
    val tableName = "dbname.tablename"
    var command = MySQLDataTransporter.getLoadSQL(tableName, loadSetting)
    command = command.replace("\n","").replace("\r", "").replaceAll("[ ]+"," ")
    command must include (s"LOAD DATA LOCAL INFILE '${loadSetting.location.getPath}' INTO TABLE $tableName")
    command must include (s"FIELDS TERMINATED BY '${loadSetting.delimiter}'")
  }

}
