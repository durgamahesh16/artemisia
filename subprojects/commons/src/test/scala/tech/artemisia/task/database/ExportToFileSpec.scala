package tech.artemisia.task.database

import java.io.{File, FileOutputStream}

import tech.artemisia.TestSpec
import tech.artemisia.core.Keywords
import tech.artemisia.task.settings.{BasicExportSetting, DBConnection}
import tech.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 4/28/16.
 */
class ExportToFileSpec extends TestSpec {

  val table = "export_to_file"
  val testDbInterface = TestDBInterFactory.withDefaultDataLoader(table)
  val connectionProfile = DBConnection("","","","default", 1000)
  val file = new File(this.getClass.getResource("/exports/ExportToFile.txt").getFile)
  val exportSettings = BasicExportSetting(delimiter = 0x1, header = true)

  "ExportToFile" must "export query result to file" in {
    val exportToFile = new ExportToFile(name = "ExportToFileTest",
    sql = s"select * from $table",
    file.toURI,
    connectionProfile,
    exportSettings
    ) {
      override val dbInterface: DBInterface = testDbInterface
      override val target = Left(new FileOutputStream(new File(location)))
    }
    val config = exportToFile.execute()
    config.as[Int](s"ExportToFileTest.${Keywords.TaskStats.STATS}.rows") must be (2)
    scala.io.Source.fromFile(file).getLines().toList(2) must be ("2\u0001bar\u0001FALSE\u0001100\u000110000000\u00018723.38\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00.0")
  }


}
