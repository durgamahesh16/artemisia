package tech.artemisia.task.database.mysql

import com.typesafe.config.Config
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.Util


class SQLRead(name: String = Util.getUUID, sql: String, connectionProfile: DBConnection)
  extends tech.artemisia.task.database.SQLRead(name, sql, connectionProfile) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile)

}

object SQLRead extends TaskLike {

  override val taskName = database.SQLRead.taskName

  override val info = database.SQLRead.info

  override def apply(name: String, config: Config) = database.SQLRead.create[SQLRead](name, config)

  override val desc: String = database.SQLRead.desc

  override def configStructure(component: String): String = database.SQLRead.configStructure(component)

  override val fieldDefinition: Seq[String] = database.SQLRead.fieldDefinition

}