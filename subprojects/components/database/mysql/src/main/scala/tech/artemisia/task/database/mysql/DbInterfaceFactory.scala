package tech.artemisia.task.database.mysql

import java.security.InvalidParameterException
import java.sql.{DriverManager, Connection}

import tech.artemisia.task.database.{DefaultDataTransporter, DBInterface}
import tech.artemisia.task.settings.ConnectionProfile


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
  def getInstance(connectionProfile: ConnectionProfile, mode: String = "default") = {
    mode match {
      case "default" => new DefaultDBInterface(connectionProfile)
      case "bulk" => new NativeDBInterface(connectionProfile)
      case _ => throw new InvalidParameterException(s"$mode is not supported")
    }
  }

  /**
   * MySQL DBInterface with default Loader
   *
   * @param connectionProfile ConnectionProfile object
   */
  class DefaultDBInterface(connectionProfile: ConnectionProfile) extends DBInterface with DefaultDataTransporter {
    override def connection: Connection = {
      getConnection(connectionProfile)
    }
  }

  /**
   * MySQL DBInterface with native Loader
   *
   * @param connectionProfile ConnectionProfile object
   */
  class NativeDBInterface(connectionProfile: ConnectionProfile) extends DBInterface with MySQLDataTransporter {
    override def connection: Connection = {
      getConnection(connectionProfile)
    }
  }

  private def getConnection(connectionProfile: ConnectionProfile) = {
    DriverManager.getConnection(s"jdbc:mysql://${connectionProfile.hostname}/${connectionProfile.default_database}?" +
      s"user=${connectionProfile.username}&password=${connectionProfile.password}")
  }
  
}



