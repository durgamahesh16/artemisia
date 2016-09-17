package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.TestSpec
import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 9/14/16.
  */

class TPTScriptGeneratorSpec extends TestSpec {


  "TPTScriptGenerator" must "generate load operator parameters" in {
    new TPTScriptGenerator {
      override protected val loadSetting = TPTLoadSetting(errorLimit = 1000)
      override protected val tptLoadConfig: TPTLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override val tptScript: String = ""
      loadOperAtts("ERRORTABLE1") must be("VARCHAR", "database.table_ET")
      loadOperAtts("TARGETTABLE") must be("VARCHAR", "database.table")
      loadOperAtts("TRACELEVEL") must be("VARCHAR", "None")
      loadOperAtts("LOGTABLE") must be("VARCHAR", "database.table_LG")
      loadOperAtts("PACK") must be("INTEGER", "2000")
      loadOperAtts("ERRORTABLE2") must be("VARCHAR", "database.table_UV")
      loadOperAtts("WORKTABLE") must be("VARCHAR", "database.table_WT")
      loadOperAtts("ERRORLIMIT") must be("INTEGER", "1000")
      loadOperAtts("DropErrorTable") must be("VARCHAR", "Yes")
      loadOperAtts("PACKMAXIMUM") must be("VARCHAR", "No")
    }
  }


  it must "generate dataconnector operator parameters with no quoting" in {
    new TPTScriptGenerator {
      override protected val loadSetting = TPTLoadSetting(errorLimit = 1000, delimiter = '\t')
      override protected val tptLoadConfig: TPTLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override val tptScript: String = ""
      dataConnAttrs("VALIDUTF8") must be("VARCHAR", "UTF8")
      dataConnAttrs("NAMEDPIPETIMEOUT") must be("INTEGER", "120")
      dataConnAttrs("REPLACEMENTUTF8CHAR") must be("VARCHAR", " ")
      dataConnAttrs("FILENAME") must be("VARCHAR", "input.pipe")
      dataConnAttrs("TEXTDELIMITERHEX") must be("VARCHAR", "9")
      dataConnAttrs("INDICATORMODE") must be("VARCHAR", "N")
      dataConnAttrs("OPENMODE") must be("VARCHAR", "Read")
      dataConnAttrs("FORMAT") must be("VARCHAR", "DELIMITED")
      dataConnAttrs("DIRECTORYPATH") must be("VARCHAR", "/var/path")
      dataConnAttrs("SKIPROWS") must be("INTEGER", "0")
      dataConnAttrs("BUFFERSIZE") must be("INTEGER", "524288")
      dataConnAttrs must not contain "QUOTEDDATA"
      dataConnAttrs must not contain "ESCAPEQUOTEDELIMITER"
      dataConnAttrs must not contain "OPENQUOTEMARK"
    }
  }

  it must "generate dataconnector operator parameters with quoting" in {
    new TPTScriptGenerator {
      override protected val loadSetting = TPTLoadSetting(errorLimit = 1000, delimiter = '|',
        quoting = true, quotechar = '~', escapechar = '-')
      override protected val tptLoadConfig: TPTLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override val tptScript: String = ""
      dataConnAttrs("QUOTEDDATA") must be("VARCHAR", "Optional")
      dataConnAttrs("ESCAPEQUOTEDELIMITER") must be("VARCHAR", "-")
      dataConnAttrs("OPENQUOTEMARK") must be("VARCHAR", "~")
      dataConnAttrs("TEXTDELIMITERHEX") must be("VARCHAR", "7c")
    }
  }


  it must "must generate schema" in {
    new TPTScriptGenerator {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override val tptScript: String = ""
      override protected val loadSetting = TPTLoadSetting()
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      this.schemaDefinition.split(System.lineSeparator) must contain theSameElementsInOrderAs
        Seq("\"col1_1\" VARCHAR(25)",",\"col2_2\" VARCHAR(25)")
    }
  }

  it must "must generate insert column list" in {
    new TPTScriptGenerator {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override val tptScript: String = ""
      override protected val loadSetting = TPTLoadSetting()
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      insertColumnList must be (
        """"col1"
          |,"col2"""".stripMargin)
    }
  }

  it must "must generate value column list" in {
    new TPTScriptGenerator {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override val tptScript: String = ""
      override protected val loadSetting = TPTLoadSetting()
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

  it must "must generate select list" in {
    new TPTScriptGenerator {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override val tptScript = ""
      override protected val loadSetting = TPTLoadSetting()
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "N"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      selectColumnList must be (""""col1_1" as "col1_1",
                                  |"col2_2" as "col2_2"""".stripMargin)
    }
  }


  it must "must generate select list with custom null string" in {
    new TPTScriptGenerator {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override val tptScript = ""
      override protected val loadSetting = TPTLoadSetting(nullString= Some("\\T"))
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "N"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      selectColumnList must be (""""col1_1" as "col1_1",
                                  |CASE WHEN "col2_2" ='\T' THEN NULL ELSE "col2_2" END as "col2_2"""".stripMargin)
    }
  }

}
