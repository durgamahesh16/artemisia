package tech.artemisia.task.database

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.inventory.exceptions.SettingNotFoundException
import tech.artemisia.task.Task
import tech.artemisia.task.settings.{DBConnection, ExportSetting}
import tech.artemisia.util.HoconConfigUtil.Handler
import tech.artemisia.util.DocStringProcessor._

import scala.reflect.ClassTag

/**
 * Created by chlr on 4/13/16.
 */

/**
  *
  * @param name name of the task instance
  * @param sql query for the export
  * @param connectionProfile Connection Profile settings
  * @param exportSettings Export settings
  */
abstract class ExportToFile(name: String, val sql: String, val connectionProfile: DBConnection ,val exportSettings: ExportSetting)
  extends Task(name: String) {

     val dbInterface: DBInterface

     override protected[task] def setup(): Unit = {}

     /**
      *
      * SQL export to file
       *
       * @return Config object with key rows and values as total number of rows exports
      */
     override protected[task] def work(): Config = {
       val records = dbInterface.export(sql, exportSettings)
       wrapAsStats {
         ConfigFactory parseString
           s"""
             | rows = $records
           """.stripMargin
       }
     }

     override protected[task] def teardown(): Unit = {}

}

object ExportToFile  {

  val taskName = "ExportToFile"

  val info = "export query results to a file"

  def doc(component: String, defaultPort: Int) =
    s"""
      | $taskName task is used to export SQL query results to a file.
      | The typical task $taskName configuration is as shown below
      |
      | {
      |  Component = $component
      |  Task =  $taskName
      |  params = {
      |    dsn = <%
      |           connection-name
      |           <------------->
      |           ${DBConnection.structure(defaultPort).ident(15)}
      |          %>
      |    export = ${ExportSetting.structure.ident(15)}
      |    <%
      |      sql = "SELECT * FROM TABLE"
      |      <-------------------------->
      |      sqlfile = run_queries.sql
      |    %> @required
      | }
      |
      |Its param include
      | dsn =  either a name of the dsn or a config-object with username/password and other credentials
      | export:
      |       ${ExportSetting.fieldDescription.ident(8)}
      |
    """.stripMargin

  /**
    *
    * @param name task name
    * @param config task configuration
    */
  def create[T <: ExportToFile : ClassTag](name: String, config: Config): ExportToFile = {
    val exportSettings = ExportSetting(config.as[Config]("export"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val sql =
      if (config.hasPath("sql")) config.as[String]("sql")
      else if (config.hasPath("sqlfile")) config.asFile("sqlfile")
      else throw new SettingNotFoundException("sql/sqlfile key is missing")
    implicitly[ClassTag[T]].runtimeClass.getConstructor(classOf[String], classOf[String], classOf[DBConnection],
      classOf[ExportSetting]).newInstance(name, sql, connectionProfile, exportSettings).asInstanceOf[ExportToFile]
  }

}



