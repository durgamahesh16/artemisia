package tech.artemisia.task.database

import tech.artemisia.task.settings.LoadSetting

/**
 * Created by chlr on 7/10/16.
 */

trait DBImporter {

  self: DBInterface =>

  def load(tableName: String, loadSetting: LoadSetting): (Long,Long)

} 
