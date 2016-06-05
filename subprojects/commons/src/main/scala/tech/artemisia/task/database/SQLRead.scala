package tech.artemisia.task.database

import com.typesafe.config.{ConfigRenderOptions, Config}
import tech.artemisia.core.AppLogger
import tech.artemisia.task.Task
import tech.artemisia.task.settings.ConnectionProfile
import tech.artemisia.util.Util

/**
 * Created by chlr on 4/22/16.
 */

/**
 *
 * @param name name of the task
 * @param sql query to be executed
 * @param connectionProfile connection profile to use
 */
abstract class SQLRead(name: String = Util.getUUID, sql: String, connectionProfile: ConnectionProfile)
  extends Task(name) {

  val dbInterface: DBInterface

  override protected[task] def setup(): Unit = {}

  /**
   * execute query and parse as config file.
   * considers only the first row of the query
   * @return config file object
   */
  override protected[task] def work(): Config = {
    val result = dbInterface.queryOne(sql)
    AppLogger debug s"query result ${result.root().render(ConfigRenderOptions.concise())}"
    result
  }

  override protected[task] def teardown(): Unit = {}

}

object SQLRead {

  /**
    * @return one line description of the task
    */
  def info = "execute select queries and wraps the results in config"

}
