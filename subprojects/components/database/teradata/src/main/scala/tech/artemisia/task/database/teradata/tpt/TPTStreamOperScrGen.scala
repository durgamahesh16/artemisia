package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 9/18/16.
  */
class TPTStreamOperScrGen(override val tptLoadConfig: TPTLoadConfig,
                          override val loadSetting: TPTLoadSetting,
                          override implicit val dbConnection: DBConnection) extends TPTLoadScriptGen {

  override protected val loadType: String = "STREAM"

  override protected def preExecuteSQL: String = {
    s"'drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName};'"
  }

  override protected def targetAttributes: Map[String, (String, String)] = {
    baseTargetAttributes ++ Map (
      "AppendErrorTable" -> ("VARCHAR", "Yes"),
      "ERRORTABLE" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_ET"),
      "DropErrorTable" -> ("VARCHAR","No")
    )
  }
}
