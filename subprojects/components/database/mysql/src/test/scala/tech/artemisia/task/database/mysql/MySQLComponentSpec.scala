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
        |     ${Keywords.Connection.PORT} = -1
        |  }
        |  sql = "SELECT * FROM table"
        |}
      """.stripMargin

    val task = component.dispatchTask("SQLRead", "sql_read", config).asInstanceOf[SQLRead]
    task.sql must be ("SELECT * FROM table")
    task.connectionProfile.port must be (-1)
    task.connectionProfile.default_database must be ("db")
  }

  it must "dispatch SQLExecute when request" in {

    val config = ConfigFactory parseString
      s"""
         |{
         |  dsn = {
         |     ${Keywords.Connection.HOSTNAME} = dummy_host
         |     ${Keywords.Connection.USERNAME} = user
         |     ${Keywords.Connection.PASSWORD} = pass
         |     ${Keywords.Connection.DATABASE} = db
         |     ${Keywords.Connection.PORT} = -1
         |  }
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
         |  dsn = {
         |     ${Keywords.Connection.HOSTNAME} = dummy_host
         |     ${Keywords.Connection.USERNAME} = user
         |     ${Keywords.Connection.PASSWORD} = pass
         |     ${Keywords.Connection.DATABASE} = db
         |     ${Keywords.Connection.PORT} = -1
         |  }
         |  destination-table = test_table
         |       load-setting = {
         |         header =  yes
         |         delimiter = "\\u0001"
         |         quoting = no,
         |         load-path = output.txt
         |       }
         |}
      """.stripMargin

    val task = component.dispatchTask("LoadToTable", "sql_read", config).asInstanceOf[LoadToTable]
    task.tableName must be ("test_table")
    task.loadSettings.delimiter must be ('\u0001')
    Paths.get(task.loadSettings.location).getFileName.toString must be ("output.txt")
  }


  it must "dispatch ExportToFile when request" in {
    val config = ConfigFactory parseString
      s"""
        | {
        |    dsn = {
        |       ${Keywords.Connection.HOSTNAME} = dummy_host
        |       ${Keywords.Connection.USERNAME} = user
        |       ${Keywords.Connection.PASSWORD} = pass
        |       ${Keywords.Connection.DATABASE} = db
        |       ${Keywords.Connection.PORT} = -1
        |     }
        |    export = {
        |      delimiter = "\\t"
        |      file = output.txt
        |      header = yes
        |    }
        |    sql = "select * from dual"
        |  }
      """.stripMargin
    val task = component.dispatchTask("ExportToFile", "sql_read", config).asInstanceOf[ExportToFile]
    task.sql must be ("select * from dual")
    task.exportSettings.delimiter must be ('\t')
    task.exportSettings.header must be (true)
  }


}
