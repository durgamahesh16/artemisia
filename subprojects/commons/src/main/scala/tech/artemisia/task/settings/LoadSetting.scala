package tech.artemisia.task.settings

/**
  * Created by chlr on 7/1/16.
  */

/**
  * Base Abstract LoadSetting object
  * @param skipRows no of rows to skip in load file
  * @param delimiter delimiter of the load file
  * @param quoting is file quoted
  * @param quotechar quote char used to enclose fields.
  * @param escapechar escape character to escape special symbols
  * @param truncate truncate target table
  * @param mode mode of operation.
  * @param batchSize size of batch insert
  * @param errorTolerance tolerance factor records rejection during load
  */
abstract class LoadSetting(val skipRows: Int = 0,
                           override val delimiter: Char = ',',
                           override val quoting: Boolean = false,
                           override val quotechar: Char = '"',
                           override val escapechar: Char = '\\',
                           val truncate: Boolean = false,
                           val mode: String = "default",
                           val batchSize: Int = 100,
                           val errorTolerance: Option[Double] = None)
                extends CSVSettings(delimiter, quoting, quotechar, escapechar) {

  def setting: String

}
