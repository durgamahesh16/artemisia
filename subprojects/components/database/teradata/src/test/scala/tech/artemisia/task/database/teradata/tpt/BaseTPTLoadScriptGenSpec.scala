package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.TestSpec
import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 9/14/16.
  */

class BaseTPTLoadScriptGenSpec extends TestSpec {


  "LoadScriptGenerator" must "generate load operator parameters" in {
    new BaseTPTLoadScriptGen {
      override protected val loadSetting = TPTLoadSetting(errorLimit = 1000)
      override protected val tptLoadConfig: TPTLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override protected val targetAttributes: Map[String, (String, String)] = Map(
        "ERRORTABLE" -> ("VARCHAR", "database.table_ET"),
        "ERRORLIMIT" -> ("INTEGER", "1000"),
        "TARGETTABLE" -> ("VARCHAR", "database.table")
      )
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      targetAttributes("ERRORTABLE") must be("VARCHAR", "database.table_ET")
      targetAttributes("TARGETTABLE") must be("VARCHAR", "database.table")
      targetAttributes("ERRORLIMIT") must be("INTEGER", "1000")
    }
  }


  it must "generate dataconnector operator parameters with no quoting" in {
    new BaseTPTLoadScriptGen {
      override protected val loadSetting = TPTLoadSetting(errorLimit = 1000, delimiter = '\t')
      override protected val tptLoadConfig: TPTLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      sourceAttributes("VALIDUTF8") must be("VARCHAR", "UTF8")
      sourceAttributes("NAMEDPIPETIMEOUT") must be("INTEGER", "120")
      sourceAttributes("REPLACEMENTUTF8CHAR") must be("VARCHAR", " ")
      sourceAttributes("FILENAME") must be("VARCHAR", "input.pipe")
      sourceAttributes("TEXTDELIMITERHEX") must be("VARCHAR", "9")
      sourceAttributes("INDICATORMODE") must be("VARCHAR", "N")
      sourceAttributes("OPENMODE") must be("VARCHAR", "Read")
      sourceAttributes("FORMAT") must be("VARCHAR", "DELIMITED")
      sourceAttributes("DIRECTORYPATH") must be("VARCHAR", "/var/path")
      sourceAttributes("SKIPROWS") must be("INTEGER", "0")
      sourceAttributes("BUFFERSIZE") must be("INTEGER", "524288")
      sourceAttributes must not contain "QUOTEDDATA"
      sourceAttributes must not contain "ESCAPEQUOTEDELIMITER"
      sourceAttributes must not contain "OPENQUOTEMARK"
    }
  }

  it must "generate dataconnector operator parameters with quoting" in {
    new BaseTPTLoadScriptGen {
      override protected val loadSetting = TPTLoadSetting(errorLimit = 1000, delimiter = '|',
        quoting = true, quotechar = '~', escapechar = '-')
      override protected val tptLoadConfig: TPTLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      sourceAttributes("QUOTEDDATA") must be("VARCHAR", "Optional")
      sourceAttributes("ESCAPEQUOTEDELIMITER") must be("VARCHAR", "-")
      sourceAttributes("OPENQUOTEMARK") must be("VARCHAR", "~")
      sourceAttributes("TEXTDELIMITERHEX") must be("VARCHAR", "7c")
    }
  }


  it must "generate schema" in {
    new BaseTPTLoadScriptGen {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override protected val loadSetting = TPTLoadSetting()
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      this.schemaDefinition.split(System.lineSeparator) must contain theSameElementsInOrderAs
        Seq("\"col1_1\" VARCHAR(25)",",\"col2_2\" VARCHAR(25)")
    }
  }

  it must "generate insert column list" in {
    new BaseTPTLoadScriptGen {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override protected val loadSetting = TPTLoadSetting()
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      insertColumnList must be (
        """"col1"
          |,"col2"""".stripMargin)
    }
  }

  it must "generate value column list" in {
    new BaseTPTLoadScriptGen {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override protected val loadSetting = TPTLoadSetting()
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      valueColumnList must be (
        """:col1_1
          |,:col2_2""".stripMargin
      )
    }
  }

  it must "generate select list" in {
    new BaseTPTLoadScriptGen {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override protected val loadSetting = TPTLoadSetting()
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "N"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      selectColumnList must be (""""col1_1" as "col1_1",
                                  |"col2_2" as "col2_2"""".stripMargin)
    }
  }


  it must "generate select list with custom null string" in {
    new BaseTPTLoadScriptGen {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override protected val loadSetting = TPTLoadSetting(nullString= Some("\\T"))
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "N"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      selectColumnList must be (""""col1_1" as "col1_1",
                                  |CASE WHEN "col2_2" ='\T' THEN NULL ELSE "col2_2" END as "col2_2"""".stripMargin)
    }
  }

}
