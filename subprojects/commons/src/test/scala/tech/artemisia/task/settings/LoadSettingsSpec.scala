package tech.artemisia.task.settings

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec

/**
 * Created by chlr on 4/30/16.
 */

class LoadSettingsSpec extends TestSpec {

  "LoadSetting" must "properly construct object from Config" in {
    val config = ConfigFactory parseString
      """
        | {
        | load-path = export.dat
        |	header = yes
        | skip-lines = 10
        |	delimiter = "\t"
        |	quoting = no
        |	escapechar = "\\"
        |	quotechar = "\""
        |}
      """.stripMargin
    val setting = LoadSettings(config)
    setting.escapechar must be ('\\')
    setting.skipRows must be (10)
    setting.delimiter must be ('\t')
    setting.quotechar must be ('"')
    setting.location.toString must be ("export.dat")
  }
}
