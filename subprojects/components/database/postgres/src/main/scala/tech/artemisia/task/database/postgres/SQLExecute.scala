package tech.artemisia.task.database.postgres

import com.typesafe.config.Config
import tech.artemisia.task.database.DBInterface
import tech.artemisia.task.settings.DBConnection
import tech.artemisia.task.{TaskLike, database}

/**
  * Created by chlr on 6/11/16.
  */

class SQLExecute(name: String, sql: String, connectionProfile: DBConnection) extends
  database.SQLExecute(name, sql, connectionProfile) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile)

  /**
    * No work is done in this phase
    */
  override protected[task] def setup(): Unit = {}

  /**
    * No work is done in this phase
    */
  override protected[task] def teardown(): Unit = {}

}

object SQLExecute extends TaskLike {

  override val taskName = database.SQLExecute.taskName

  override def apply(name: String, config: Config) = database.SQLExecute.create[SQLExecute](name, config)

  override val info = database.SQLExecute.info

  override val desc: String = database.SQLExecute.desc

  override def configStructure(component: String): String = database.SQLExecute.configStructure(component)

  override val fieldDefinition: Seq[String] = database.SQLExecute.fieldDefinition
}
