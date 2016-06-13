package tech.artemisia.task.database

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.inventory.exceptions.SettingNotFoundException
import tech.artemisia.task.settings.{ConnectionProfile, ExportSetting}
import tech.artemisia.task.{Task, TaskLike}
import tech.artemisia.util.HoconConfigUtil.Handler

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
abstract class ExportToFile(name: String, val sql: String, val connectionProfile: ConnectionProfile ,val exportSettings: ExportSetting)
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

object ExportToFile extends TaskLike {

  override val taskName = "ExportToFile"

  override val info = "export query results to a file"

  override def doc(component: String) =
    s"""
      | $taskName task is used to export SQL query results to a file.
      | The typical task $taskName configuration is as shown below
      |
      | {
      |  Component = $component
      |  Task =  $taskName
      |  params = {
      |    dsn = ?
      |    export = {
      |       file = ?
      |	      header = false
      |	      delimiter = ","
      |	      quoting = no,
      |	      quotechar = "\""
      |       escapechar = "\\"
      |     }
      |    [sql|sqlfile] = ?
      | }
      |
      |Its param include
      | dsn =  either a name of the dsn or a config-object with username/password and other credentials
      | export:
      |   file =  location of the file to which data is to be exported. eg: /var/tmp/output.txt
      |   header = boolean literal to enable/disable header
      |   delimiter = character to be used for delimiter
      |   quoting = boolean literal to enable/disable quoting of fields.
      |   quotechar = quotechar to use if quoting is enabled.
      |   escapechar = escape character use for instance to escape delimiter values in field
      |   sql = SQL query whose resultset will be exported.
      |   sqlfile = used in place of sql key to pass the file containing the SQL
      |
    """.stripMargin

  override def apply(name: String, config: Config) = ???

  /**
    *
    * @param name task name
    * @param config task configuration
    */
  def create[T <: ExportToFile : ClassTag](name: String, config: Config): ExportToFile = {
    val exportSettings = ExportSetting(config.as[Config]("export"))
    val connectionProfile = ConnectionProfile.parseConnectionProfile(config.getValue("dsn"))
    val sql =
      if (config.hasPath("sql")) config.as[String]("sql")
      else if (config.hasPath("sqlfile")) config.asFile("sqlfile")
      else throw new SettingNotFoundException("sql/sqlfile key is missing")
    implicitly[ClassTag[T]].runtimeClass.getConstructor(classOf[String], classOf[String], classOf[ConnectionProfile],
      classOf[ExportSetting]).newInstance(name, sql, connectionProfile, exportSettings).asInstanceOf[ExportToFile]
  }

}



