package tech.artemisia.task.database.mysql

import com.typesafe.config.Config
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.ConnectionProfile
import tech.artemisia.util.Util


class SQLRead(name: String = Util.getUUID, sql: String, connectionProfile: ConnectionProfile)
  extends tech.artemisia.task.database.SQLRead(name, sql, connectionProfile) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile)

}

object SQLRead extends TaskLike {

  override val taskName = database.SQLRead.taskName

  override val info = database.SQLRead.info

  override def doc(component: String) = database.SQLRead.doc(component)

  override def apply(name: String, config: Config) = database.SQLRead.create[SQLRead](name, config)

}