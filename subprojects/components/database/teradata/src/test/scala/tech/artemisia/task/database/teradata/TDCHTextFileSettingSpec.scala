package tech.artemisia.task.database.teradata

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec
import tech.artemisia.task.database.teradata.tdch.TDCHTextSetting

/**
  * Created by chlr on 8/31/16.
  */
class TDCHTextFileSettingSpec extends TestSpec {


  "TDCHTextFileSetting" must "construct object from config" in {
    val config = ConfigFactory parseString
      """
        |{
        |  delimiter = ","
        |  quoting = yes
        |  quote-char = "\""
        |  null-string = "\\N"
        |  escape-char = "\\"
        |}
      """.stripMargin
    val setting = TDCHTextSetting(config)
    setting.delimiter must be (',')
    setting.quoting mustBe true
    setting.quoteChar must be ('"')
    setting.escapedBy must be ('\\')
    setting.nullString.get must be ("\\N")

    setting.commandArgs must contain theSameElementsAs List("-separator", "\\u00f7", "-enclosedby", '"',
      "-escapedby", '\\', "-nullstring", "\\N")

  }

}
