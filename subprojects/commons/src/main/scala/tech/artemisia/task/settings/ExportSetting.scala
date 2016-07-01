package tech.artemisia.task.settings

import java.net.URI

/**
  * Created by chlr on 7/1/16.
  */
abstract class ExportSetting(val file: URI, val header: Boolean = false, override val delimiter: Char = ',',
                    override val quoting: Boolean = false, override val quotechar: Char = '"',
                    override val escapechar: Char = '\\', val mode: String = "default")
  extends CSVSettings(delimiter, quoting, quotechar, escapechar)
