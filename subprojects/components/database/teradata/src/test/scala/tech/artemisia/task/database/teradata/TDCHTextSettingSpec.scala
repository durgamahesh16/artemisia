package tech.artemisia.task.database.teradata

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec

/**
  * Created by chlr on 9/2/16.
  */
class TDCHTextSettingSpec extends TestSpec {

  "TDCHTextSetting" must "parse config and construct object" in {
    val config = ConfigFactory parseString
      s"""
         | {
         |   delimiter = "|"
         |   quoting = yes
         |   quote-char = "a"
         |   escape-char = "b"
         | }
       """.stripMargin
    val setting = TDCHTextSetting(config)
    setting.delimiter must be ('|')
    setting.quoting mustBe true
    setting.quoteChar must be ('a')
    setting.escapedBy must be ('b')
    setting.commandArgs must contain only ("-separator", "\\u00f7", "-enclosedby", 'a', "-escapedby", 'b')
  }

}
