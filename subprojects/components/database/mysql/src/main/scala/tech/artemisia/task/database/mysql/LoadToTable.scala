package tech.artemisia.task.database.mysql

import com.typesafe.config.Config
import tech.artemisia.task.TaskLike
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.{DBConnection, LoadSettings}
import tech.artemisia.util.Util
import tech.artemisia.task.database

/**
 * Created by chlr on 4/30/16.
 */
class LoadToTable(name: String = Util.getUUID, tableName: String, connectionProfile: DBConnection, loadSettings: LoadSettings)
  extends database.LoadToTable(name, tableName, connectionProfile, loadSettings) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile, loadSettings.mode)

  /**
   * No operations are done in this phase
   */
  override protected[task] def setup(): Unit = {}

  /**
   * No operations are done in this phase
   */
  override protected[task] def teardown(): Unit = {}

}

object LoadToTable extends TaskLike {

  override val info = database.LoadToTable.info

  override def doc(component: String) = database.LoadToTable.doc(component, 3306)

  override val taskName = database.LoadToTable.taskName

  override def apply(name: String, config: Config) = database.LoadToTable.create[LoadToTable](name, config)

}
