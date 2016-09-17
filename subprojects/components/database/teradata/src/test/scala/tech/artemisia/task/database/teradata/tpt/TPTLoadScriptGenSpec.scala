package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.TestSpec
import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 9/16/16.
  */
class TPTLoadScriptGenSpec extends TestSpec {

  "TPTLoadScriptGenerator" must "generate TPT scripts" in {
    val generator = new TPTLoadScriptGenerator(
      TPTLoadConfig("database", "table", "/var/path", "input.pipe"),
      TPTLoadSetting(dataConnectorAttrs = Map("ROWERRFILENAME" -> ("VARCHAR","/var/path/errorfile"))),
      DBConnection("td_server", "voltron", "password", "dbc", 1025)
    ) {
      override lazy val dbInterface = ???
      override lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "N"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
    }
    generator.tptScript.replaceAll("\\s","") must be (TPTLoadScriptGenSpec.tptScript1.replaceAll("\\s",""))
  }

}

object TPTLoadScriptGenSpec {

  val tptScript1 =
    s"""|USING CHARACTER SET UTF8
        |DEFINE JOB load_database_table (
        |    DEFINE OPERATOR tpt_writer
        |    TYPE LOAD
        |    SCHEMA *
        |    ATTRIBUTES
        |    (
        |        VARCHAR UserName,
        |        VARCHAR UserPassword,
        |        VARCHAR TdpId,
        |        VARCHAR ERRORTABLE1 = 'database.table_ET',
        |        VARCHAR TARGETTABLE = 'database.table',
        |        VARCHAR TRACELEVEL = 'None',
        |        VARCHAR LOGTABLE = 'database.table_LG',
        |        INTEGER PACK = 2000,
        |        VARCHAR ERRORTABLE2 = 'database.table_UV',
        |        VARCHAR WORKTABLE = 'database.table_WT',
        |        INTEGER ERRORLIMIT = 2000,
        |        VARCHAR DropErrorTable = 'Yes',
        |        VARCHAR PACKMAXIMUM = 'No'
        |    );
        |    DEFINE SCHEMA W_0_sc_load_database_table
        |    (
        |        "col1_1" VARCHAR(25)
        |        ,"col2_2" VARCHAR(25)
        |    );
        |    DEFINE OPERATOR tpt_reader
        |    TYPE DATACONNECTOR PRODUCER
        |    SCHEMA W_0_sc_load_database_table
        |    ATTRIBUTES
        |    (
        |        VARCHAR VALIDUTF8 = 'UTF8',
        |        INTEGER NAMEDPIPETIMEOUT = 120,
        |        VARCHAR REPLACEMENTUTF8CHAR = ' ',
        |        VARCHAR FILENAME = 'input.pipe',
        |        VARCHAR TEXTDELIMITERHEX = '2c',
        |        VARCHAR INDICATORMODE = 'N',
        |        VARCHAR OPENMODE = 'Read',
        |        VARCHAR FORMAT = 'DELIMITED',
        |        VARCHAR DIRECTORYPATH = '/var/path',
        |        VARCHAR ROWERRFILENAME = '/var/path/errorfile',
        |        INTEGER SKIPROWS = 0,
        |        INTEGER BUFFERSIZE = 524288
        |    );
        |    DEFINE OPERATOR DDL_OPERATOR ()
        |    DESCRIPTION 'DDL Operator'
        |    TYPE DDL
        |    ATTRIBUTES
        |    (
        |        VARCHAR UserName = 'voltron',
        |        VARCHAR UserPassword = 'password',
        |        VARCHAR ARRAY ErrorList = ['2580','3807','3916'],
        |        VARCHAR TdpId = 'td_server:1025'
        |    );
        |    Step DROP_TABLE
        |    (
        |        APPLY
        |        'drop table database.table_WT;',
        |        'drop table database.table_ET;',
        |        'drop table database.table_UV;',
        |        'drop table database.table_LG;'
        |        TO OPERATOR (DDL_OPERATOR);
        |    );
        |    Step LOAD_TABLE
        |    (
        |        APPLY
        |        (
        |            'INSERT INTO database.table (
        |                   "col1"
        |                   ,"col2"
        |            ) VALUES (
        |                   :col1_1
        |                   ,:col2_2
        |            );'
        |        )
        |        TO OPERATOR
        |        (
        |            tpt_writer[1]
        |            ATTRIBUTES
        |            (
        |            UserName = 'voltron',
        |            UserPassword = 'password',
        |            TdpId = 'td_server:1025'
        |            )
        |        )
        |          SELECT
        |                  "col1_1" as "col1_1",
        |                   "col2_2" as "col2_2"
        |          FROM OPERATOR(
        |              tpt_reader[1]
        |          );
        |      );
        |   );
     """.stripMargin

}
