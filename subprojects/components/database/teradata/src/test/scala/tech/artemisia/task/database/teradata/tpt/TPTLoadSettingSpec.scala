package tech.artemisia.task.database.teradata.tpt

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec

/**
  * Created by chlr on 9/14/16.
  */
class TPTLoadSettingSpec extends TestSpec {


  "TPTLoadSetting" must "construct itself from config object" in {

    val config = ConfigFactory parseString
       """
         | {
         |    delimiter = ","
         |    quoting = yes
         |    quotechar = "'"
         |    escapechar = "\\"
         |    mode = default
         |    batch-size = 10000
         |    error-limit = 200
         |    truncate = yes
         |    header = yes
         |    bulk-threshold = 1K
         |    load-attrs = {
         |        OPENMODE = WRITE
         |        BUFFERSIZE = {
         |           type = "INTEGER"
         |           value = "9876543"
         |        }
         |    }
         |    dtconn-attrs = {
         |        OPENMODE = WRITE
         |        BUFFERSIZE = {
         |           type = "INTEGER"
         |           value = "9876543"
         |         }
         |      }
         |    skip-lines = 10
         | }
         |
       """.stripMargin

    val setting = TPTLoadSetting(config)
    setting.delimiter must be (',')
    setting.quoting mustBe true
    setting.quotechar must be ('\'')
    setting.escapechar must be ('\\')
    setting.mode must be ("default")
    setting.bulkLoadThreshold must be (1024)
    setting.batchSize must be (10000)
    setting.errorLimit must be (200)
    setting.truncate mustBe true
    setting.loadOperatorAttrs must be (Map("OPENMODE" -> ("VARCHAR","WRITE"), "BUFFERSIZE" -> ("INTEGER","9876543")))
    setting.dataConnectorAttrs must be (Map("OPENMODE" -> ("VARCHAR","WRITE"), "BUFFERSIZE" -> ("INTEGER","9876543")))
  }


  it must "throw an exception when an unknown mode is set" in {
    val exception = intercept[IllegalArgumentException] {
      TPTLoadSetting(mode = "unknow_mode")
    }
    exception.getMessage must be ("requirement failed: unknow_mode is not supported. supported modes are default,fastload,auto")
  }

}
