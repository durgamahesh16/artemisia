package tech.artemisia.task.database.teradata

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec

/**
 * Created by chlr on 7/12/16.
 */

class TeraLoadSettingSpec extends TestSpec {

  "TeraLoadSetting" must "" in {
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
      |   mode = default
      |   session = 20
      |   recreate-table = yes
      |}
    """.stripMargin
    val setting = TeraLoadSetting(config)
    setting.recreateTable mustBe true
    setting.sessions must be (20)
  }

}
