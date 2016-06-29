package tech.artemisia.task.database.teradata

import java.sql.{Connection, DriverManager}

import tech.artemisia.task.database.{DBInterface, DefaultDataTransporter}
import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 6/26/16.
  */

/**
  * Factory object for constructing Dbinterface object
  */
object DbInterfaceFactory {

  /**
    *
    * @param connectionProfile ConnectionProfile object
    * @param mode mode can be either `default` or `native` to choose loader method
    * @return DbInterface
    */
  def getInstance(connectionProfile: DBConnection, mode: String = "default") = {
    mode match {
      case "default" => new DefaultDBInterface(connectionProfile)
      case "fastload" => new NativeDBInterface(connectionProfile, mode)
      case "fastexport" => new NativeDBInterface(connectionProfile, mode)
      case _ => throw new IllegalArgumentException(s"mode '$mode' is not supported")
    }
  }

  /**
    * Teradata DBInterface with default Loader/Exporter
    *
    * @param connectionProfile ConnectionProfile object
    */
  class DefaultDBInterface(connectionProfile: DBConnection) extends DBInterface with DefaultDataTransporter {
    override def connection: Connection = {
      getConnection(connectionProfile)
    }
  }

  /**
    * Teradata DBInterface with specialized Fastload/FastExport
    *
    * @param connectionProfile ConnectionProfile object
    */
  class NativeDBInterface(connectionProfile: DBConnection, mode: String) extends DBInterface with TDDataTransporter {
    override def connection: Connection = {
      getConnection(connectionProfile, Some(mode))
    }
  }


  private def getConnection(connectionProfile: DBConnection, mode: Option[String] = None) = {
    DriverManager.getConnection(s"""jdbc:teradata://${connectionProfile.hostname}/${connectionProfile.default_database}," +
      s"dbs_port=${connectionProfile.port}${mode.map(x => s",type=$x").getOrElse("")}"""
      ,connectionProfile.username
      ,connectionProfile.password)
  }

}