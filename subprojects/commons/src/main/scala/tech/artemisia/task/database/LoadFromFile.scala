package tech.artemisia.task.database

import java.io.{File, InputStream}
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.core.AppLogger
import tech.artemisia.task.Task
import tech.artemisia.task.settings.{DBConnection, LoadSetting}
import tech.artemisia.util.HoconConfigUtil.Handler

import scala.reflect.ClassTag

/**
 * Created by chlr on 4/30/16.
 */

/**
 * An abstract task to load data into a table
 *
 * @param name name for the task
 * @param tableName destination table to be loaded
 * @param connectionProfile connection details for the database
 * @param loadSetting load setting details
 */
abstract class LoadFromFile(val name: String, val tableName: String, val location: URI, val connectionProfile: DBConnection,
                            val loadSetting: LoadSetting) extends Task(name) {

  val dbInterface: DBInterface

  /**
    * inputStream to read data from
    */
  val source: Either[InputStream, URI]

  /**
    * list of supported modes
    */
  protected val supportedModes: Seq[String]

  override protected[task] def setup() = {
    require(supportedModes contains loadSetting.mode, s"unsupported mode ${loadSetting.mode}")
    if (loadSetting.truncate) {
      AppLogger info s"truncating table $tableName"
      dbInterface.execute(s"DELETE FROM $tableName",printSQL =  false)
    }
  }

  /**
   * Actual data export is done in this phase.
   * Number of records loaded is emitted in stats node
    *
    * @return any output of the work phase be encoded as a HOCON Config object.
   */
  override def work(): Config = {
    val (totalRows, rejectedCnt) = dbInterface.loadTable(tableName, source, loadSetting)
    AppLogger info s"${totalRows - rejectedCnt} rows loaded into table $tableName"
    AppLogger info s"$rejectedCnt row were rejected"
    wrapAsStats {
      ConfigFactory parseString
        s"""
           |loaded = ${totalRows-rejectedCnt}
           |rejected = $rejectedCnt
         """.stripMargin
    }
  }

   override protected[task] def teardown() = {
    AppLogger debug s"closing database connection"
    dbInterface.terminate()
     source match {
       case Left(stream) => AppLogger debug s"closing InputStream from ${location.toString}"
                            stream.close()
       case Right(_) => ()
     }
   }
}

object LoadFromFile {

  val taskName = "SQLLoad"

  val info = "load a file into a table"

  val desc: String =
    s"""
      |$taskName task is used to load content into a table typically from a file.
      |the configuration object for this task is as shown below.
    """.stripMargin

  def paramConfigDoc(defaultPort: Int) = {
   val config = ConfigFactory parseString s"""
       | "dsn_[1]" = connection-name
       |  destination-table = "dummy_table @required"
       |  location = /var/tmp/file.txt
     """.stripMargin
    config
      .withValue("load",BasicLoadSetting.structure.root())
      .withValue(""""dsn_[2]"""",DBConnection.structure(defaultPort).root())
  }

  val fieldDefinition = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "destination-table" -> "destination table to load",
    "location" -> "path pointing to the source file",
    s"load" -> BasicLoadSetting.fieldDescription
  )


  def create[T <: LoadFromFile : ClassTag](name: String, config: Config): LoadFromFile = {
      val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
      val destinationTable = config.as[String]("destination-table")
      val loadSettings = BasicLoadSetting(config.as[Config]("load"))
      val location = new File(config.as[String]("load-path")).toURI
      implicitly[ClassTag[T]].runtimeClass.asSubclass(classOf[LoadFromFile]).getConstructor(classOf[String],
        classOf[String], classOf[URI], classOf[DBConnection], classOf[BasicLoadSetting]).newInstance(name, destinationTable,
        location ,connectionProfile, loadSettings)
  }

}
