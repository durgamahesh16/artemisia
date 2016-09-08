package tech.artemisia.task.database.teradata

import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 9/6/16.
  */

class TPTLoadScriptGenerator (val database: String ,val targetTable: String, loadSetting: TPTLoadSetting, dbConnection: DBConnection) {


  private val defaultLoadOperAtts = Map(
    "TRACELEVEL" -> ("VARCHAR" -> "None"),
    "PACK" -> ("INTEGER" -> "2000"),
    "PACKMAXIMUM" -> ("VARCHAR" -> "No"),
    "ERRORLIMIT" -> ("INTEGER" -> "2000"),
    "DropErrorTable" -> ("VARCHAR" -> "Yes")
  )


  private val defaultDataConnAttrs = Map(
    "INDICATORMODE" -> ("VARCHAR","N"),
    "FORMAT" -> ("VARCHAR", "DELIMITED"),
    "VALIDUTF8" -> ("VARCHAR", "UTF8"),
    "BUFFERSIZE" -> ("INTEGER", "524288")
  )

  private val _script =
    s"""
       |USING CHARACTER SET UTF8
       |DEFINE JOB load_$targetTable (
       |    DEFINE OPERATOR tpt_writer
       |    TYPE LOAD
       |    SCHEMA *
       |    ATTRIBUTES
       |    (
       |        VARCHAR UserName,
       |        VARCHAR UserPassword,
       |        VARCHAR TdpId,
       |        $loadOperatorAttributes
       |    );
       |    DEFINE SCHEMA W_0_sc_update_chlr_test2
       |    (
       |        $schemaDefinition
       |    );
       |    DEFINE SCHEMA Boolean_conversion
       |    (
       |    );
       |    DEFINE OPERATOR tpt_reader
       |    TYPE DATACONNECTOR PRODUCER
       |    SCHEMA W_0_sc_load_$targetTable
       |    ATTRIBUTES
       |    (
       |        $dataConnectorAttributes
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
       |        'drop table $targetTable;',
       |        'drop table ${targetTable}_WT;',
       |        'drop table ${targetTable}_ET;',
       |        'drop table ${targetTable}_UV;',
       |        'drop table ${targetTable}_LG;'
       |        TO OPERATOR (DDL_OPERATOR);
       |    );
       |    Step LOAD_TABLE
       |    (
       |        APPLY
       |        (
       |            'INSERT INTO sandbox.chlr_test2 (
       |                   $insertColumnList
       |            ) VALUES (
       |                   $valueColumnList
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
       |                  $selectColumnList
       |          FROM OPERATOR(
       |              tpt_reader[1]
       |          );
       |      );
       |   );
     """.stripMargin


  def schemaDefinition = {}

  def loadOperatorAttributes = {}

  def dataConnectorAttributes = {}

  def insertColumnList = {}

  def valueColumnList = {}

  def selectColumnList = {}



}
