package tech.artemisia.task.database.postgres

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.util.Util

/**
  * Created by chlr on 6/11/16.
  */

class SQLRead(name: String = Util.getUUID, sql: String, connectionProfile: DBConnection)
  extends database.SQLRead(name, sql, connectionProfile) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile)

}

object SQLRead extends TaskLike {

  override val taskName = database.SQLRead.taskName

  override val defaultConfig = ConfigFactory.empty()

  override val info = database.SQLRead.info

  override def apply(name: String, config: Config) = database.SQLRead.create[SQLRead](name, config)

  override val desc: String = database.SQLRead.desc

  override val paramConfigDoc = database.SQLRead.paramConfigDoc(5432)

  override val fieldDefinition = database.SQLRead.fieldDefinition
}