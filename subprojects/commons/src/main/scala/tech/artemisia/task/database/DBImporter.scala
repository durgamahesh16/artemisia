package tech.artemisia.task.database


import java.io.InputStream
import java.net.URI

import tech.artemisia.task.settings.LoadSetting

/**
 * Created by chlr on 7/10/16.
 */

trait DBImporter {

  self: DBInterface =>

  def load(tableName: String, inputStream: InputStream, loadSetting: LoadSetting): (Long,Long)

  def load(tableName: String, location: URI, loadSetting: LoadSetting): (Long, Long)

} 
