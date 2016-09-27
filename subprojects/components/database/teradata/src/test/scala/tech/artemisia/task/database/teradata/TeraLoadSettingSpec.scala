package tech.artemisia.task.database.teradata

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec

/**
 * Created by chlr on 7/12/16.
 */

class TeraLoadSettingSpec extends TestSpec {

  "TeraLoadSetting" must "contruct itself from config" in {
    val config = ConfigFactory parseString
    """
      |{
      |   load-path = "/var/tmp"
      |	  header =  no
      |	  skip-lines = 0
      |	  delimiter = ","
      |	  quoting = no
      |	  quotechar = "\""
      |   escapechar = "\\"
      |   truncate = false
      |   batch-size = 100
      |   bulk-threshold = 1K
      |   mode = default
      |}
    """.stripMargin withFallback TeraLoadSetting.defaultConfig
    val setting = TeraLoadSetting(config)
    setting.bulkLoadThreshold must be (1024)
    setting.delimiter must be (',')
    setting.quoting mustBe false
    setting.quotechar must be ('\"')
  }

}
