package tech.artemisia.task.settings

import java.io.File

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec

/**
 * Created by chlr on 4/16/16.
 */
class ExportSettingsSpec extends TestSpec {

  "ExportSetting" must "properly construct object from Config" in {
      val config = ConfigFactory parseString
        """
          | {
          | file = export.dat
          |	header = yes
          |	delimiter = "\t"
          |	quoting = no
          |	escapechar = "\\"
          |	quotechar = "\""
          |}
        """.stripMargin withFallback BasicExportSetting.defaultConfig
    val setting = BasicExportSetting(config)
    setting.escapechar must be ('\\')
    setting.header must be (true)
    setting.delimiter must be ('\t')
    setting.quotechar must be ('"')
    new File(setting.file).getName must be ("export.dat")
  }
}
