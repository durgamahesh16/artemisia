package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 9/18/16.
  */

/**
  * This class generates the TPT stream
  *
  * @param tptLoadConfig
  * @param loadSetting
  * @param dbConnection
  */
class TPTStreamOperScrGen(override val tptLoadConfig: TPTLoadConfig,
                          override val loadSetting: TPTLoadSetting,
                          override implicit val dbConnection: DBConnection) extends TPTLoadScriptGen {

  override protected val loadType: String = "STREAM"

  override protected val preExecuteSQLs = Seq(s"DROP TABLE ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_ET;")

  override protected val targetAttributes: Map[String, (String, String)] = Map(
      "AppendErrorTable" -> ("VARCHAR", "Yes"),
      "ERRORTABLE" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_ET"),
      "DropErrorTable" -> ("VARCHAR","Yes"),
      "ArraySupport" -> ("VARCHAR", "On"),
      "ErrorLimit" -> ("INTEGER", loadSetting.errorLimit.toString),
      "LogTable" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_LG"),
      "PackMaximum" -> ("VARCHAR","Yes")
    )
}
