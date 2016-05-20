package tech.artemesia.task.database.mysql

import java.security.InvalidParameterException
import java.sql.{DriverManager, Connection}

import tech.artemesia.task.database.{DataLoader, DBInterface}
import tech.artemesia.task.settings.ConnectionProfile


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
      case "default" => new DefualtDBInterface(connectionProfile)
      case "native" => new NativeDBInterface(connectionProfile)
      case _ => throw new InvalidParameterException(s"$mode is not supported")
    }
  }

  /**
   * MySQL DBInterface with defualt Loader
   * @param connectionProfile ConnectionProfile object
   */
  class DefualtDBInterface(connectionProfile: ConnectionProfile) extends DBInterface with DataLoader {
    override def connection: Connection = {
      getConnection(connectionProfile)
    }
  }

  /**
   * MySQL DBInterface with native Loader
   * @param connectionProfile ConnectionProfile object
   */
  class NativeDBInterface(connectionProfile: ConnectionProfile) extends DBInterface with MySQLDataLoader {
    override def connection: Connection = {
      getConnection(connectionProfile)
    }
  }

  private def getConnection(connectionProfile: ConnectionProfile) = {
    DriverManager.getConnection(s"jdbc:mysql://${connectionProfile.hostname}/${connectionProfile.default_database}?" +
      s"user=${connectionProfile.username}&password=${connectionProfile.password}")
  }
  
}



