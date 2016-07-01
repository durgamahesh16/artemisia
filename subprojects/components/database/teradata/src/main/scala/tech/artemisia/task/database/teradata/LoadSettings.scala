package tech.artemisia.task.database.teradata

import java.net.URI

import tech.artemisia.task.settings

/**
  * Created by chlr on 6/30/16.
  */
case class LoadSettings(override val location: URI, override val skipRows: Int = 0, override val delimiter: Char = ',',
                        override val quoting: Boolean = false, override val quotechar: Char = '"', override val escapechar: Char = '\\',
                        override val mode: String = "default", override val batchSize: Int = 100, override val rejectFile: Option[String] = None,
                        override val errorTolerance: Option[Double] = None, sessions: Int)
  extends settings.LoadSettings(location, skipRows, delimiter, quoting, quotechar, escapechar, mode, batchSize, rejectFile, errorTolerance) {

}
