package tech.artemisia.task.database.postgres

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.{BasicLoadSetting, DBConnection}
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.util.Util

/**
  * Created by chlr on 6/11/16.
  */


class LoadToTable(name: String = Util.getUUID, tablename: String, connectionProfile: DBConnection, loadSettings: BasicLoadSetting)
  extends database.LoadToTable(name, tablename, connectionProfile, loadSettings) {

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

  override val taskName = database.LoadToTable.taskName

  override def apply(name: String, config: Config) = database.LoadToTable.create[LoadToTable](name, config)

  override val desc: String = database.LoadToTable.desc

  override val paramConfigDoc = database.LoadToTable.paramConfigDoc(5432)

  override val fieldDefinition = database.LoadToTable.fieldDefinition

}
