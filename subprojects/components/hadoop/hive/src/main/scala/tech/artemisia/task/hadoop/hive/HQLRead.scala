package tech.artemisia.task.hadoop.hive

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}

/**
  * Created by chlr on 8/2/16.
  */
class HQLRead(taskName: String, sql: String, connectionProfile: DBConnection) extends
                        database.SQLRead(taskName, sql, connectionProfile) {

  override val dbInterface: DBInterface = DBInterfaceFactory.getDBInterface(connectionProfile)

}

object HQLRead extends TaskLike {

  override val taskName: String = "HQLRead"

  override val defaultConfig: Config = ConfigFactory.empty()

  override def apply(name: String, config: Config) = database.SQLRead.create[HQLRead](name, config)

  override val info = database.SQLRead.info

  override val desc: String = database.SQLRead.desc

  override val paramConfigDoc = database.SQLRead.paramConfigDoc(10000)

  override val fieldDefinition = database.SQLRead.fieldDefinition

}
