package tech.artemisia.task.database

import tech.artemisia.task.settings.ExportSetting

/**
 * Created by chlr on 7/10/16.
 */

trait DBDataExporter {

  self: DBInterface =>

  /**
   * export data
   * @return number of records exported
   */
  def export(sql: String, exportSetting: ExportSetting): Long

}
