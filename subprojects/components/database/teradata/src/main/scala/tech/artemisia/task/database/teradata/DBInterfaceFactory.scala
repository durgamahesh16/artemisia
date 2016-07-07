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
    * @param mode              mode can be either `default` or `native` to choose loader method
    * @return DbInterface
    */
  def getInstance(connectionProfile: DBConnection, mode: String = "default", session: Int = 1) = {
    mode match {
      case "default" => new DefaultDBInterface(connectionProfile, None, 1)
      case "fastload" => new TeraDBInterface(connectionProfile, Some("fastload"), session)
      case "fastexport" => new TeraDBInterface(connectionProfile, Some("fastexport"), session)
      case _ => throw new IllegalArgumentException(s"mode '$mode' is not supported")
    }
  }

  class DefaultDBInterface(connectionProfile: DBConnection, mode: Option[String], session: Int) extends DBInterface with DefaultDataTransporter {
    override def getNewConnection: Connection = {
      getConnection(connectionProfile, mode, session)
    }
  }

  /**
    * Teradata DBInterface with specialized Fastload/FastExport
    *
    * @param connectionProfile ConnectionProfile object
    */
  class TeraDBInterface(connectionProfile: DBConnection, mode: Option[String], session: Int) extends DBInterface with BulkDataTransporter {
    override def getNewConnection: Connection = {
        getConnection(connectionProfile, mode, session)
    }
  }


  private def getConnection(connectionProfile: DBConnection, mode: Option[String], session: Int) = {
    DriverManager.getConnection(
      s"""jdbc:teradata://${connectionProfile.hostname}/${connectionProfile.default_database}," +
      s"dbs_port=${connectionProfile.port}${mode.map(x => s",type=$x").getOrElse("")},SESSIONS=${session}"""
      , connectionProfile.username
      , connectionProfile.password)
  }



}