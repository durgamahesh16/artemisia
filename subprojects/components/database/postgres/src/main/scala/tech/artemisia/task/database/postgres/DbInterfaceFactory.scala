package tech.artemisia.task.database.postgres

import java.sql.{Connection, DriverManager}
import tech.artemisia.task.database.{DBInterface, DefaultDataTransporter}
import tech.artemisia.task.settings.DBConnection


/**
  * Created by chlr on 4/13/16.
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
      case "bulk" => new NativeDBInterface(connectionProfile)
      case _ => throw new IllegalArgumentException(s"mode '$mode' is not supported")
    }
  }

  /**
    * Postgres DBInterface with default Loader
    *
    * @param connectionProfile ConnectionProfile object
    */
  class DefaultDBInterface(connectionProfile: DBConnection) extends DBInterface with DefaultDataTransporter {
    override def connection: Connection = {
      getConnection(connectionProfile)
    }
  }

  /**
    * Postgres DBInterface with default Loader
    *
    * @param connectionProfile ConnectionProfile object
    */
  class NativeDBInterface(connectionProfile: DBConnection) extends DBInterface with PGDataTransporter {
    override def connection: Connection = {
      getConnection(connectionProfile)
    }
  }


  class BulkDBInterFace()


  private def getConnection(connectionProfile: DBConnection) = {
    DriverManager.getConnection(s"jdbc:postgresql://${connectionProfile.hostname}/${connectionProfile.default_database}?" +
      s"user=${connectionProfile.username}&password=${connectionProfile.password}")
  }

}



