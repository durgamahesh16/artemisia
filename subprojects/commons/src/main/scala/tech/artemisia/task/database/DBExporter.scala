package tech.artemisia.task.database

import java.io.OutputStream
import java.net.URI

import tech.artemisia.task.settings.ExportSetting

/**
 * Created by chlr on 7/10/16.
 */

trait DBExporter {

  self: DBInterface =>

  /**
    *
    * @param sql SELECT query to execute
    * @param outputStream outputstream
    * @param exportSetting export settings
    * @return
    */
  def export(sql: String, outputStream: OutputStream, exportSetting: ExportSetting): Long

  /**
    *
    * @param sql SELECT query to execute
    * @param location
    * @param exportSetting
    * @return
    */
  def export(sql: String, location: URI, exportSetting: ExportSetting): Long

}
