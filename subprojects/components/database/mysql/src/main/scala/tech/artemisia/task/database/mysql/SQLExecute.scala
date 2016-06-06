package tech.artemisia.task.database.mysql

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.ConnectionProfile
import tech.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 5/21/16.
 */

class SQLExecute(name: String, sql: String, connectionProfile: ConnectionProfile) extends
                      tech.artemisia.task.database.SQLExecute(name, sql, connectionProfile) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile)

  /**
   * No work is done in this phase
   */
  override protected[task] def setup(): Unit = {}

  /**
   * No work is done in this phase
   */
  override protected[task] def teardown(): Unit = {}

}

object SQLExecute {

  def apply(name: String, config: Config) = {
    new SQLExecute(name,
    sql = config.as[String]("sql"),
    connectionProfile = ConnectionProfile.parseConnectionProfile(config.getValue("dsn"))
    )
  }

  /**
    * @return one line description of the task
    */
  def info = tech.artemisia.task.database.SQLExecute.info


  /**
    * brief description of the task
    */
  val doc = tech.artemisia.task.database.SQLExecute.doc(classOf[MySQLComponent].getSimpleName)

}
