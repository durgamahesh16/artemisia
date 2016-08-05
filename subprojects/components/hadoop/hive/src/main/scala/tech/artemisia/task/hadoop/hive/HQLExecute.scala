package tech.artemisia.task.hadoop.hive

import com.typesafe.config.{Config, ConfigFactory}
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}
import tech.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/1/16.
  */

class HQLExecute(override val taskName: String, override val sql: String, connectionProfile: Option[DBConnection])
    extends database.SQLExecute(taskName, sql, connectionProfile.getOrElse(DBConnection.getDummyConnection)) {

  override val dbInterface: DBInterface = DBInterfaceFactory.getDBInterface(connectionProfile)

  override protected[task] def setup(): Unit = {}

}

object HQLExecute extends TaskLike {

  override val taskName: String = "HQLExecute"

  override def paramConfigDoc: Config =  database.SQLExecute.paramConfigDoc(10000)

  override def defaultConfig: Config =  ConfigFactory.empty()

  override def fieldDefinition: Map[String, AnyRef] = database.SQLExecute.fieldDefinition

  override def apply(name: String, config:  Config) = {
    val sql = config.asInlineOrFile("hql")
    val connection = config.hasPath("dsn") match {
      case true => Some(DBConnection.parseConnectionProfile(config.getValue("dsn")))
      case false => None
    }
    new HQLExecute(name, sql, connection)
  }

  override val info: String = "Execute Hive HQL queries"

  override val desc: String = ""

}
