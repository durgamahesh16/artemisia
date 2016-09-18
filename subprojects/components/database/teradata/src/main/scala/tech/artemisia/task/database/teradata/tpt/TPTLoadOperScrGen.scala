package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 9/6/16.
  */

class TPTLoadOperScrGen(override val tptLoadConfig: TPTLoadConfig,
                        override val loadSetting: TPTLoadSetting,
                        override implicit val dbConnection: DBConnection) extends TPTLoadScriptGen {

  override protected val loadType: String = "LOAD"

  override protected def preExecuteSQL: String = {
    s"""|'drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_WT;',
        |'drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_ET;',
        |'drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_UV;',
        |'drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_LG;'""".stripMargin
  }

  override protected def targetAttributes: Map[String, (String, String)] = {
    baseTargetAttributes ++ Map (
      "ERRORTABLE1" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_ET"),
      "ERRORTABLE2" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_UV"),
      "WORKTABLE" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_WT"),
      "DropErrorTable" -> ("VARCHAR","Yes")
    )
  }

}
