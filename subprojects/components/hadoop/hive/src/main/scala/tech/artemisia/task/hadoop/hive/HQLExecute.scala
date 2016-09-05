package tech.artemisia.task.hadoop.hive

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.util.CommandUtil._
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/1/16.
  */

class HQLExecute(override val taskName: String, override val sql: String, connectionProfile: Option[DBConnection])
    extends database.SQLExecute(taskName, sql, connectionProfile.getOrElse(DBConnection.getDummyConnection)) {

  protected lazy val hiveCli = getExecutablePath("hive") map {
    x => new HiveCLIInterface(x)
  } match {
    case Some(x) => x
    case None => throw new RuntimeException(s"hive executable not found in path")
  }

  override protected[task] def setup(): Unit = {}

  override lazy val dbInterface: DBInterface = connectionProfile match {
    case Some(profile) => new HiveServerDBInterface(profile)
    case None => throw new RuntimeException("HiveServer2 interface being accessed when it is not defined")
  }

  override def work() = {
    connectionProfile match {
      case Some(profile) => super.work()
      case None => {
        wrapAsStats {
          val result = hiveCli.execute(sql, taskName)
          ConfigFactory.empty()
                  .withValue("loaded", result.root())
        }
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

object HQLExecute extends TaskLike {

  override val taskName: String = "HQLExecute"

  override def paramConfigDoc: Config =  database.SQLExecute.paramConfigDoc(10000)
                                                .withValue(""""dsn_[1]"""",ConfigValueFactory.fromAnyRef("connection-name @optional"))
                                                .withValue(""""dsn_[2]"""",DBConnection.structure(10000).root())

  override def defaultConfig: Config =  ConfigFactory.empty()

  override def fieldDefinition: Map[String, AnyRef] = database.SQLExecute.fieldDefinition

  override def apply(name: String, config:  Config) = {
    val sql = config.asInlineOrFile("sql")
    val connection = config.hasPath("dsn") match {
      case true => Some(DBConnection.parseConnectionProfile(config.getValue("dsn")))
      case false => None
    }
    new HQLExecute(name, sql, connection)
  }

  override val info: String = "Execute Hive HQL queries"

  override val desc: String = ""

}
