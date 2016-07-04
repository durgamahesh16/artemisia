package tech.artemisia.task.database.teradata

import tech.artemisia.task.database.{DBInterface, DefaultDataTransporter}
import tech.artemisia.task.settings.LoadSetting

/**
  * Created by chlr on 7/2/16.
  */

trait BulkDataTransporter extends DefaultDataTransporter {

  self: DBInterface =>

  override def loadData(tableName: String, loadSetting: LoadSetting) = {
    assert(loadSetting.location.getScheme == "file",
      s"schema ${loadSetting.location.getScheme} is not supported. file:// is the only supported schema")
    val dbWriter = new FastLoadDBWriter(tableName, loadSetting, this)
    this.rowInsert(dbWriter, loadSetting)
  }

}
