package tech.artemisia.task.database.teradata

import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.DocStringProcessor.StringUtil

/**
  * Created by chlr on 9/6/16.
  */

class TPTLoadScriptGenerator(tptLoadConfig: TPTLoadConfig, loadSetting: TPTLoadSetting, dbConnection: DBConnection) {

  protected val dbInterface = DBInterfaceFactory.getInstance(dbConnection)

  protected lazy val tableMetadata = TeraUtils.tableMetadata(tptLoadConfig.databaseName, tptLoadConfig.tableName, dbInterface)

  private val loadOperAtts = Map(
    "TRACELEVEL" -> ("VARCHAR" -> "None"),
    "PACK" -> ("INTEGER" -> "2000"),
    "PACKMAXIMUM" -> ("VARCHAR" -> "No"),
    "ERRORLIMIT" -> ("INTEGER" -> "2000"),
    "DropErrorTable" -> ("VARCHAR" -> "Yes"),
    "ERRORTABLE1" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_ET"),
    "ERRORTABLE2" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_UV"),
    "WORKTABLE" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_WT"),
    "TARGETTABLE" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}"),
    "LOGTABLE" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_LG")
  )


  private val dataConnAttrs = Map(
    "OPENMODE" -> ("VARCHAR", "Read"),
    "TEXTDELIMITERHEX" -> ("VARCHAR" -> Integer.toHexString(loadSetting.delimiter.toInt)),
    "DIRECTORYPATH" -> ("VARCHAR" -> tptLoadConfig.directory),
    "NAMEDPIPETIMEOUT" -> ("INTEGER" -> "120"),
    "FILENAME" -> ("VARCHAR" -> tptLoadConfig.fileName),
    "REPLACEMENTUTF8CHAR" -> ("VARCHAR" -> " "),
    "INDICATORMODE" -> ("VARCHAR","N"),
    "FORMAT" -> ("VARCHAR", "DELIMITED"),
    "VALIDUTF8" -> ("VARCHAR", "UTF8"),
    "BUFFERSIZE" -> ("INTEGER", "524288"),
    "ROWERRFILENAME" -> ("VARCHAR", loadSetting.errorFile)
  )

  def tptScript =
    s"""
       |USING CHARACTER SET UTF8
       |DEFINE JOB load_${tptLoadConfig.databaseName}_${tptLoadConfig.tableName} (
       |    DEFINE OPERATOR tpt_writer
       |    TYPE LOAD
       |    SCHEMA *
       |    ATTRIBUTES
       |    (
       |        VARCHAR UserName,
       |        VARCHAR UserPassword,
       |        VARCHAR TdpId,
       |        ${loadOperatorAttributes.ident(8)}
       |    );
       |    DEFINE SCHEMA W_0_sc_load_${tptLoadConfig.databaseName}_${tptLoadConfig.tableName}
       |    (
       |        ${schemaDefinition.ident(8)}
       |    );
       |    DEFINE OPERATOR tpt_reader
       |    TYPE DATACONNECTOR PRODUCER
       |    SCHEMA W_0_sc_load_${tptLoadConfig.databaseName}_${tptLoadConfig.tableName}
       |    ATTRIBUTES
       |    (
       |        ${dataConnectorAttributes.ident(8)}
       |    );
       |    DEFINE OPERATOR DDL_OPERATOR ()
       |    DESCRIPTION 'DDL Operator'
       |    TYPE DDL
       |    ATTRIBUTES
       |    (
       |        VARCHAR UserName = '${dbConnection.username}',
       |        VARCHAR UserPassword = '${dbConnection.password}',
       |        VARCHAR ARRAY ErrorList = ['2580','3807','3916'],
       |        VARCHAR TdpId = '${dbConnection.hostname}:${dbConnection.port}'
       |    );
       |    Step DROP_TABLE
       |    (
       |        APPLY
       |        'drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_WT;',
       |        'drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_ET;',
       |        'drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_UV;',
       |        'drop table ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_LG;'
       |        TO OPERATOR (DDL_OPERATOR);
       |    );
       |    Step LOAD_TABLE
       |    (
       |        APPLY
       |        (
       |            'INSERT INTO ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName} (
       |                   ${insertColumnList.ident(19)}
       |            ) VALUES (
       |                   ${valueColumnList.ident(19)}
       |            );'
       |        )
       |        TO OPERATOR
       |        (
       |            tpt_writer[1]
       |            ATTRIBUTES
       |            (
       |            UserName = '${dbConnection.username}',
       |            UserPassword = '${dbConnection.password}',
       |            TdpId = '${dbConnection.hostname}:${dbConnection.port}'
       |            )
       |        )
       |          SELECT
       |                  ${selectColumnList.ident(19)}
       |          FROM OPERATOR(
       |              tpt_reader[1]
       |          );
       |      );
       |   );
     """.stripMargin


  def schemaDefinition = tableMetadata map { x => s""""${x._4}" VARCHAR(${x._3})""" } mkString "\n,"

  def loadOperatorAttributes = {
    loadOperAtts ++ loadSetting.loadOperatorAttrs map {
      case (attrName, (attrType, attrValue))
        if attrType.toUpperCase.trim == "VARCHAR" => s"VARCHAR $attrName = '$attrValue'"
      case (attrName, (attrType, attrValue)) => s"$attrType $attrName = $attrValue"
    } mkString ",\n"
  }

  def dataConnectorAttributes = {
    dataConnAttrs ++ loadSetting.dataConnectorAttrs map {
      case (attrName, (attrType, attrValue))
        if attrType.toUpperCase.trim == "VARCHAR" => s"VARCHAR $attrName = '$attrValue'"
      case (attrName, (attrType, attrValue)) => s"$attrType $attrName = $attrValue"
    } mkString ",\n"
  }

  def insertColumnList = tableMetadata map { x => s""""${x._1}""""} mkString "\n,"

  def valueColumnList = tableMetadata map { x => s":${x._4}"} mkString "\n,"

  def selectColumnList = {
      tableMetadata map { x => x._5.trim.toUpperCase flatMap {
        case 'N' => s""""${x._4}" as "${x._4}""""
        case _ =>
          s"""CASE WHEN "${x._4}" ='<!N!>' THEN NULL ELSE "${x._4}" END as "${x._4}"""".stripMargin
      }
    } mkString ",\n"
  }

}
