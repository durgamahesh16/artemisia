package tech.artemisia.task.hadoop.hive

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.util.CommandUtil._
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/2/16.
  */
class HQLRead(taskName: String, sql: String, connectionProfile: Option[DBConnection]) extends
                        database.SQLRead(taskName, sql, connectionProfile.getOrElse(DBConnection.getDummyConnection)) {


  protected lazy val hiveCli = getExecutablePath("hive") map {
    x => new HiveCLIInterface(x)
  } match {
    case Some(x) => x
    case None => throw new RuntimeException(s"hive executable not found in path")
  }

  override lazy val dbInterface: DBInterface = connectionProfile match {
    case Some(profile) => new HiveServerDBInterface(profile)
    case None => throw new RuntimeException("HiveServer2 interface being accessed when it is not defined")
  }


  override def work() = {
    connectionProfile match {
      case Some(profile) => super.work()
      case None => {
        hiveCli.queryOne(sql, taskName)
      }
    }
  }


  override def teardown() = {
    connectionProfile match {
      case Some(profile) => super.teardown()
      case _ => ()
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
