package tech.artemisia.task.database.postgres

import tech.artemisia.TestSpec
import tech.artemisia.task.database.{BasicExportSetting, BasicLoadSetting}

/**
 * Created by chlr on 6/14/16.
 */
class PGDataTransporterSpec extends TestSpec {


  "PGDataTransporter" must "compile copy command for export" in {

    val sql = "select * from dummy_table"
    val exportSetting = BasicExportSetting(delimiter = '\t', quoting = true, quotechar = '"')
    var command = PGDataTransporter.getExportCmd(sql, exportSetting)
    command = command.replace("\n"," ").replace("\r"," ").replaceAll("""[ ]+"""," ")

    command must include (s"COPY ($sql)")
    command must include (s"DELIMITER '${exportSetting.delimiter}'")
    command must include (s"QUOTE '${exportSetting.quotechar}'")
    command must include (s"ESCAPE '${exportSetting.escapechar}'")
    command must include (s"FORCE_QUOTE *")

  }

  it must "compile copy command for load" in {

    val destinationTable = "dummy_table"
    val exportSetting = BasicLoadSetting(delimiter = '\t', quoting = true, quotechar = '"')
    var command = PGDataTransporter.getLoadCmd(destinationTable, exportSetting)
    command = command.replace("\n"," ").replace("\r"," ").replaceAll("""[ ]+"""," ")
    command must include (s"COPY $destinationTable")
    command must include (s"DELIMITER '${exportSetting.delimiter}'")
    command must include (s"FORMAT csv")
    command must include (s"QUOTE '${exportSetting.quotechar}'")
    command must include (s"ESCAPE '${exportSetting.escapechar}'")
    command must include (s"""HEADER ${if(exportSetting.skipRows == 0) "OFF" else "ON"}""")

  }

}
