package tech.artemisia.task.database

import java.io.{File, FileInputStream}
import java.net.URI

import com.typesafe.config.ConfigRenderOptions
import tech.artemisia.TestSpec
import tech.artemisia.task.settings.{BasicLoadSetting, DBConnection}
import tech.artemisia.util.FileSystemUtil._
import tech.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 5/18/16.
 */
class LoadToTableSpec extends TestSpec {

  "LoadToTable" must "load a file into the given table" in {
    val tableName = "LoadToTableSpec"
    withTempFile(fileName = s"${tableName}_1") {
      file => {
        // row id 106 will be rejected since quicksilver is more than the target column width (10)
        file <<=
          """|102\u0001magneto\u0001true\u0001100\u000110000000\u000187.3\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00
             |103\u0001xavier\u0001true\u0001100\u000110000000\u000187.3\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00
             |104\u0001wolverine\u0001true\u0001100\u000110000000\u000187.3\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00
             |105\u0001mystique\u0001true\u0001100\u000110000000\u000187.3\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00
             |106\u0001quicksilver\u0001true\u0001100\u000110000000\u000187.3\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00|""".stripMargin
        val loadSettings = BasicLoadSetting(delimiter = '\u0001', batchSize = 1)
        val loader = LoadToTableSpec.loader("LoadToTableSpec1",tableName, file.toURI, TestDBInterFactory.stubbedConnectionProfile,loadSettings)
        val config = loader.execute()
        info(loader.dbInterface.queryOne(s"select count(*) as cnt from $tableName").root().render(ConfigRenderOptions.concise()))
        config.as[Int]("test_task.__stats__.loaded") must be (4)
        config.as[Int]("test_task.__stats__.rejected") must be (1)
      }
    }
  }
}



object LoadToTableSpec {

  def loader(name: String, tableName: String, location: URI ,connectionProfile: DBConnection, loadSettings: BasicLoadSetting) =

    new LoadToTable("test_task",tableName, location ,connectionProfile, loadSettings) {
    override val dbInterface: DBInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    override val source = Left(new FileInputStream(new File(location)))
    override protected[task] def setup(): Unit = {}
    override protected[task] def teardown(): Unit = {}
  }

}


