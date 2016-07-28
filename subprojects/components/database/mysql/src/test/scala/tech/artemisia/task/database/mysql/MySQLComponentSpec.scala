package tech.artemisia.task.database.mysql

import java.nio.file.Paths
import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec
import tech.artemisia.core.Keywords

/**
  * Created by chlr on 6/6/16.
  */
class MySQLComponentSpec extends TestSpec {

  val component = new MySQLComponent("MySQL")

  "MySQLComponent" must "dispatch SQLRead when requested" in {
    val config = ConfigFactory parseString
      s"""
        |{
        |  dsn = {
        |     ${Keywords.Connection.HOSTNAME} = dummy_host
        |     ${Keywords.Connection.USERNAME} = user
        |     ${Keywords.Connection.PASSWORD} = pass
        |     ${Keywords.Connection.DATABASE} = db
        |  }
        |  sql = "SELECT * FROM table"
        |}
      """.stripMargin

    val task = component.dispatchTask("SQLRead", "sql_read", config).asInstanceOf[SQLRead]
    task.sql must be ("SELECT * FROM table")
    task.connectionProfile.port must be (3306)
    task.connectionProfile.default_database must be ("db")
  }

  it must "dispatch SQLExecute when request" in {

    val config = ConfigFactory parseString
      s"""
         |{
         |  ${MySQLComponentSpec.getDSN(pass = "pass")}
         |  sql = "SELECT * FROM table"
         |}
      """.stripMargin

    val task: SQLExecute = component.dispatchTask("SQLExecute", "sql_read", config).asInstanceOf[SQLExecute]
    task.sql must be ("SELECT * FROM table")
    task.connectionProfile.username must be ("user")
    task.connectionProfile.password must be ("pass")
  }


  it must "dispatch LoadToTable when request" in {


    val config = ConfigFactory parseString
      s"""
         |{
         |  ${MySQLComponentSpec.getDSN()}
         |  destination-table = test_table
         |       load-setting = {
         |         header =  yes
         |         delimiter = "\\u0001"
         |         quoting = no,
         |       }
         |  load-path = ${this.getClass.getResource("/dummy_load_file.txt").getFile}
         |}
      """.stripMargin

    val task = component.dispatchTask("SQLLoad", "sql_read", config).asInstanceOf[LoadFromFile]
    task.tableName must be ("test_table")
    task.loadSetting.delimiter must be ('\u0001')
    Paths.get(task.location.toString).getFileName.toString must be ("dummy_load_file.txt")
  }


  it must "dispatch ExportToFile when request" in {
    val config = ConfigFactory parseString
      s"""
        | {
        |  ${MySQLComponentSpec.getDSN()}
        |    export = {
        |      delimiter = "\\t"
        |      header = yes
        |    }
        |    file = output.txt
        |    sql = "select * from dual"
        |  }
      """.stripMargin
    val task = component.dispatchTask("SQLExport", "sql_read", config).asInstanceOf[ExportToFile]
    task.sql must be ("select * from dual")
    task.exportSetting.delimiter must be ('\t')
    task.exportSetting.header must be (true)
  }


  it must "spew out doc for all tasks" in {
    for (task <- component.tasks) {
      component.taskDoc(task.taskName).trim.length must be > 1
    }
  }


}


object MySQLComponentSpec {

    def getDSN(host: String = "dummy_host", user: String = "user", pass: String = "password", db: String = "db", port: Int = -1) = {
      s"""
          |dsn = {
          | ${Keywords.Connection.HOSTNAME} = $host
          | ${Keywords.Connection.USERNAME} = $user
          | ${Keywords.Connection.PASSWORD} = $pass
          | ${Keywords.Connection.DATABASE} = $db
          | ${Keywords.Connection.PORT} = $port
          |}
          |
     """.stripMargin
   }
}