package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.task.settings.DBConnection
import tech.artemisia.util.DocStringProcessor.StringUtil

/**
  * Created by chlr on 9/6/16.
  */

class TPTLoadScriptGenerator(override val tptLoadConfig: TPTLoadConfig,
                             override val loadSetting: TPTLoadSetting,
                             override val dbConnection: DBConnection) extends TPTScriptGenerator {



  override lazy val tptScript =
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

}
