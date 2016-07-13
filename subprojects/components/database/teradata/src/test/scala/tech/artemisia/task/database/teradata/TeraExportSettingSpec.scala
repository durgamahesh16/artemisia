package tech.artemisia.task.database.teradata

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec

/**
 * Created by chlr on 7/12/16.
 */


class TeraExportSettingSpec extends TestSpec {

  "TeraExportSettingSpec" must "construct object from config object" in {
    val config = ConfigFactory parseString
      """
        | {
        | file = export.dat
        |	header = yes
        |	delimiter = "\t"
        |	quoting = no
        |	escapechar = "\\"
        |	quotechar = "\""
        | session = 100
        |}
      """.stripMargin

    val setting = TeraExportSetting(config)
    setting.delimiter must be ('\t')
    setting.session must be (100)
    setting.escapechar must be ('\\')
  }

}
