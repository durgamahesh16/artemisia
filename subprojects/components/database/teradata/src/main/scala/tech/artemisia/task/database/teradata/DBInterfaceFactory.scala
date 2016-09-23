package tech.artemisia.task.database.teradata

import java.sql.{Connection, DriverManager}
import tech.artemisia.task.database.{DefaultDBBatchImporter, DefaultDBExporter, DBInterface}
import tech.artemisia.task.settings.DBConnection


/**
  * Factory object for constructing Dbinterface object
  */
object DBInterfaceFactory {

  /**
    *
    * @param connectionProfile ConnectionProfile object
    * @param mode mode can be either `default` or `native` to choose loader method
    * @return DbInterface
    */
  def getInstance(connectionProfile: DBConnection, mode: String = "default") = {
    mode match {
      case "default" => new DefaultDBInterface(connectionProfile, None)
      case "fastload" => new TeraDBInterface(connectionProfile, Some("fastload"))
      case "fastexport" => new TeraDBInterface(connectionProfile, Some("fastexport"))
      case _ => throw new IllegalArgumentException(s"mode '$mode' is not supported")
    }
  }

  class DefaultDBInterface(connectionProfile: DBConnection, mode: Option[String]) extends DBInterface
      with DefaultDBBatchImporter with DefaultDBExporter {
    override def getNewConnection: Connection = {
      getConnection(connectionProfile, mode)
    }
  }

  /**
    * Teradata DBInterface with specialized Fastload/FastExport
    *
    * @param connectionProfile ConnectionProfile object
    */
  class TeraDBInterface(connectionProfile: DBConnection, mode: Option[String]) extends DBInterface with TeraDataTransporter {
    override def getNewConnection: Connection = {
        getConnection(connectionProfile, mode)
    }
  }


  private def getConnection(connectionProfile: DBConnection, mode: Option[String]) = {
    DriverManager.getConnection(
      s"""jdbc:teradata://${connectionProfile.hostname}/${connectionProfile.default_database}," +
      s"dbs_port=${connectionProfile.port}${mode.map(x => s",type=$x").getOrElse("")}"""
      , connectionProfile.username
      , connectionProfile.password)
  }



}