package tech.artemisia.task.hadoop.hive

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/2/16.
  */
class HQLRead(taskName: String, sql: String, connectionProfile: Option[DBConnection]) extends
                        database.SQLRead(taskName, sql, connectionProfile.getOrElse(DBConnection.getDummyConnection)) {

  override val dbInterface = connectionProfile match {
    case Some(profile) => new HiveServerDBInterface(profile)
    case None => null // see if we can eliminate the usage of null
  }


  override def work() = {
    connectionProfile match {
      case Some(profile) => super.work()
      case None => {
        val dbInterface = new HiveCLIInterface()
        dbInterface.queryOne(sql, taskName)
      }
    }
  }

}

object HQLRead extends TaskLike {

  override val taskName: String = "HQLRead"

  override val defaultConfig: Config = ConfigFactory.empty()

  override def apply(name: String, config: Config) = {
    val sql = config.asInlineOrFile("sql")
    val connection = config.getAs[ConfigValue]("dsn") map DBConnection.parseConnectionProfile
    new HQLRead(name, sql, connection)
  }

  override val info = database.SQLRead.info

  override val desc: String = database.SQLRead.desc

  override val paramConfigDoc = database.SQLRead.paramConfigDoc(10000)

  override val fieldDefinition = database.SQLRead.fieldDefinition

}
