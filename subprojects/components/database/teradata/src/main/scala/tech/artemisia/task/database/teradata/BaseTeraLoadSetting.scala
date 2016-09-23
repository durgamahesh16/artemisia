package tech.artemisia.task.database.teradata

import tech.artemisia.task.settings.LoadSetting


/**
  *
  * @param skipRows no of rows to skip in load file
  * @param delimiter delimiter of the load file
  * @param quoting is file quoted
  * @param quotechar quote char used to enclose fields.
  * @param escapechar escape character to escape special symbols
  * @param truncate truncate target table
  * @param mode mode of operation.
  * @param batchSize size of batch insert
  * @param errorTolerance tolerance factor records rejection during load
  * @param bulkLoadThreshold threshold load size for fastload
  */
abstract class BaseTeraLoadSetting(override val skipRows: Int = 0,
                                   override val delimiter: Char = ',',
                                   override val quoting: Boolean = false,
                                   override val quotechar: Char = '"',
                                   override val escapechar: Char = '\\',
                                   override val truncate: Boolean = false,
                                   override val mode: String = "default",
                                   override val batchSize: Int = 100,
                                   override val errorTolerance: Option[Double] = None,
                                   val bulkLoadThreshold: Long) extends
  LoadSetting(skipRows, delimiter, quoting, quotechar, escapechar, truncate, mode, batchSize, errorTolerance)  {


  /**
    *
    * @param batchSize batch size
    * @param mode load mode
    * @return
    */
  def create(batchSize: Int, mode: String): BaseTeraLoadSetting

}

