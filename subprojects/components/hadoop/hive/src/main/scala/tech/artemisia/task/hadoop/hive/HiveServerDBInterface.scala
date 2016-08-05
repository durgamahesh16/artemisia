package tech.artemisia.task.hadoop.hive

import java.io.InputStream
import java.net.URI
import java.sql.{Connection, DriverManager}
import tech.artemisia.task.database.{DBImporter, DBInterface, DefaultDBExporter}
import tech.artemisia.task.settings.{DBConnection, LoadSetting}

/**
  * Created by chlr on 8/1/16.
  */

class HiveServerDBInterface(connectionProfile: DBConnection) extends DBInterface with DBImporter with DefaultDBExporter {

  override def getNewConnection: Connection = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    DriverManager.getConnection(s"jdbc:hive2://${connectionProfile.hostname}:${connectionProfile.port}/${connectionProfile.default_database}"
    , connectionProfile.username, connectionProfile.password)
  }

  override def load(tableName: String, inputStream: InputStream, loadSetting: LoadSetting) = ???

  override def load(tableName: String, location: URI, loadSetting: LoadSetting): (Long, Long) = ???

}
