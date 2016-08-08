package tech.artemisia.task.hadoop.hive

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec

/**
  * Created by chlr on 8/7/16.
  */
class HiveComponentSpec extends TestSpec {

  val component = new HiveComponent("Hive")

  "HiveComponent" must "dispatch HQLExecute when requested" in {
    val config = ConfigFactory parseString
      s"""
         | {
         |  sql = "select * from table"
         | }
       """.stripMargin
    val task = component.dispatchTask("HQLExecute", "task", config).asInstanceOf[HQLExecute]
    task.sql must be ("select * from table")
  }

  it must "dispatch HQLRead when requested" in {
    val config = ConfigFactory parseString
      s"""
         | {
         |  dsn = {
         |    host = hiveserver
         |    username = ironman
         |    password = stark
         |    database = db
         |    port = 1000
         |  }
         |  sql = "select * from table"
         | }
       """.stripMargin
    val task = component.dispatchTask("HQLRead", "task", config).asInstanceOf[HQLRead]
    task.sql must be ("select * from table")
    task.connectionProfile.hostname must be ("hiveserver")
    task.connectionProfile.password must be ("stark")
  }

  it must "dispatch HQLExport when requested" in {

    val config = ConfigFactory parseString
      s"""
         | {
         |  dsn = {
         |    host = hiveserver
         |    username = ironman
         |    password = stark
         |    database = db
         |    port = 1000
         |  }
         |  file = ${this.getClass.getResource("/dummy_file.txt").getFile}
         |  sql = "select * from table_export"
         | }
       """.stripMargin
    val task = component.dispatchTask("HQLExport", "task", config).asInstanceOf[HQLExport]
    task.connectionProfile.default_database must be ("db")
    task.connectionProfile.port must be (1000)
    task.sql must be ("select * from table_export")
  }

}
