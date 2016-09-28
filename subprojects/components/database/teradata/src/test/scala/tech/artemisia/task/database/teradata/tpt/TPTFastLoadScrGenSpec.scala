package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.TestSpec
import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 9/26/16.
  */
class TPTFastLoadScrGenSpec extends TestSpec {

  "TPTFastLoadScrGen" must "set correct target attributes (without quoting)" in {
    new TPTFastLoadScrGen(
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(
        skipRows = 5,
        delimiter = '\t',
        errorFile = "/var/error/file.txt"),
      DBConnection.getDummyConnection
    ) {
        sourceAttributes("VALIDUTF8") must be ("VARCHAR" -> "UTF8")
        sourceAttributes("NAMEDPIPETIMEOUT") must be ("INTEGER" -> "120")
        sourceAttributes("FILENAME") must be ("VARCHAR" -> "file")
        sourceAttributes("TEXTDELIMITERHEX") must be ("VARCHAR" -> "9")
        sourceAttributes("INDICATORMODE") must be ("VARCHAR" -> "N")
        sourceAttributes("OPENMODE") must be ("VARCHAR" -> "Read")
        sourceAttributes("FORMAT") must be ("VARCHAR" -> "DELIMITED")
        sourceAttributes("DIRECTORYPATH") must be ("VARCHAR" -> "/var/path/dir")
        sourceAttributes("ROWERRFILENAME") must be ("VARCHAR" -> "/var/error/file.txt")
        sourceAttributes("SKIPROWS") must be ("INTEGER" -> "5")
        sourceAttributes("BUFFERSIZE") must be ("INTEGER" -> "524288")
        sourceAttributes.keys must not contain "QUOTEDDATA"
        sourceAttributes.keys must not contain "ESCAPEQUOTEDELIMITER"
        sourceAttributes.keys must not contain "OPENQUOTEMARK"
    }
  }

  it must "set correct target attributes with quoting" in {
    new TPTFastLoadScrGen(
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(
        quoting = true,
        skipRows = 5,
        delimiter = '\t',
        errorFile = "/var/error/file.txt"),
      DBConnection.getDummyConnection
    ) {
        sourceAttributes("VALIDUTF8") must be ("VARCHAR" -> "UTF8")
        sourceAttributes("NAMEDPIPETIMEOUT") must be ("INTEGER" -> "120")
        sourceAttributes("FILENAME") must be ("VARCHAR" -> "file")
        sourceAttributes("TEXTDELIMITERHEX") must be ("VARCHAR" -> "9")
        sourceAttributes("INDICATORMODE") must be ("VARCHAR" -> "N")
        sourceAttributes("OPENMODE") must be ("VARCHAR" -> "Read")
        sourceAttributes("FORMAT") must be ("VARCHAR" -> "DELIMITED")
        sourceAttributes("DIRECTORYPATH") must be ("VARCHAR" -> "/var/path/dir")
        sourceAttributes("ROWERRFILENAME") must be ("VARCHAR" -> "/var/error/file.txt")
        sourceAttributes("SKIPROWS") must be ("INTEGER" -> "5")
        sourceAttributes("BUFFERSIZE") must be ("INTEGER" -> "524288")
        sourceAttributes("QUOTEDDATA") must be ("VARCHAR", "Optional")
        sourceAttributes("ESCAPEQUOTEDELIMITER") must be ("VARCHAR", "\\")
        sourceAttributes("OPENQUOTEMARK") must be ("VARCHAR", "\"")
    }
  }


  it must "set correctly set target attributes" in {
    new TPTFastLoadScrGen(
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(),
      DBConnection.getDummyConnection
    ) {
      targetAttributes("ERRORTABLE1") must be ("VARCHAR" -> "database.table_ET")
      targetAttributes("ERRORTABLE2") must be ("VARCHAR" -> "database.table_UV")
      targetAttributes("WORKTABLE") must be ("VARCHAR" -> "database.table_WT")
      targetAttributes("DropErrorTable") must be ("VARCHAR" -> "Yes")
    }
  }

  it must "generate schema" in {
    new TPTFastLoadScrGen(
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(),
      DBConnection.getDummyConnection
    ) {
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      this.schemaDefinition.split(System.lineSeparator) must contain theSameElementsInOrderAs
        Seq("\"col1_1\" VARCHAR(25)",",\"col2_2\" VARCHAR(25)")
    }
  }


  it must "must generate insert column list" in {
      new TPTFastLoadScrGen (
         TPTLoadConfig("database", "table", "/var/path/dir","file"),
         TPTLoadSetting(),
         DBConnection.getDummyConnection) {
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
    new TPTFastLoadScrGen (
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(),
      DBConnection.getDummyConnection) {
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
    new TPTFastLoadScrGen (
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(),
      DBConnection.getDummyConnection) {
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      selectColumnList must be (""""col1_1" as "col1_1",
                                  |"col2_2" as "col2_2"""".stripMargin)
    }
  }


  it must "generate select list with custom null string" in {
    new TPTFastLoadScrGen (
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(nullString = Some("\\T")),
      DBConnection.getDummyConnection) {
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "N"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      info(selectColumnList)
      selectColumnList must be (""""col1_1" as "col1_1",
                                  |CASE WHEN "col2_2" ='\T' THEN NULL ELSE "col2_2" END as "col2_2"""".stripMargin)
    }
  }


}
