package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 9/6/16.
  */

class TPTFastLoadScrGen(override val tptLoadConfig: TPTLoadConfig,
                        override val loadSetting: TPTLoadSetting,
                        override implicit val dbConnection: DBConnection) extends BaseTPTLoadScriptGen {

  override protected val loadType: String = "LOAD"

  override protected val preExecuteSQLs = Seq(
    s"drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_WT;",
    s"drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_ET;",
    s"drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_UV;",
    s"drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_LG;"
  )

  override protected val targetAttributes: Map[String, (String, String)] = Map(
      "ERRORTABLE1" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_ET"),
      "ERRORTABLE2" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_UV"),
      "WORKTABLE" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_WT"),
      "DropErrorTable" -> ("VARCHAR","Yes")
    )


}
