package tech.artemisia.task.settings

import java.net.URI

/**
  * Created by chlr on 7/1/16.
  */

abstract class LoadSetting(val location: URI, val skipRows: Int = 0, override val delimiter: Char = ',', override val quoting: Boolean = false,
                           override val quotechar: Char = '"', override val escapechar: Char = '\\', val truncate: Boolean = false, val mode: String = "default",
                           val batchSize: Int = 100, val errorTolerance: Option[Double] = None)
                extends CSVSettings(delimiter, quoting, quotechar, escapechar)
