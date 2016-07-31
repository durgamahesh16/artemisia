package tech.artemisia.util

/**
  * Created by chlr on 7/30/16.
  */

/**
  * An utility class for parsing memory literal to long
  * {{{
  *  scala> MemorySize("10MB").getBytes
  *  res5: Long = 10485760
  * }}}
  *
  * @param memory string literal of memory eg (75 kilobytes)
  */
class MemorySize(memory: String) {

  private val by = "b" :: "B" :: "byte" :: "bytes" :: Nil
  private val kb = "K" :: "k" :: "Ki" :: "KiB" :: "kB" :: "kilobyte" :: "kilobytes" :: Nil
  private val mb = "M" :: "m" :: "Mi" :: "MiB" :: "MB" :: "megabyte" :: "megabytes" :: Nil
  private val gb = "G" :: "g" :: "Gi" :: "GiB" :: "GB" :: "gigabyte" :: "gigabytes" :: Nil
  private val tb = "T" :: "t" :: "Ti" :: "TiB" :: "TB" :: "terabyte" :: "terabytes" :: Nil


  /**
    * get number of bytes in long
    * @return number of bytes in long
    */
  def toBytes: Long = {
    val rgx = (by ++ kb ++ mb ++ gb ++ tb) map { x => s"^([0-9]+)\\s*($x)$$".r }
    val matched = rgx map { _.findFirstMatchIn(memory) } collect {
      case Some(x) => x.group(1) -> x.group(2)
    }
    matched match {
      case head :: Nil => head match {
        case (x, y) if by contains y => x.toLong
        case (x, y) if kb contains y => x.toLong * 1024
        case (x, y) if mb contains y => x.toLong * 1024 * 1024
        case (x, y) if gb contains y => x.toLong * 1024 * 1024 * 1024
        case (x, y) if tb contains y => x.toLong * 1024 * 1024 * 1024 * 1024
        case _ => throw new RuntimeException(s"only bytes, kilobytes, megabytes, gigabytes, terabytes are supported")
      }
      case _ => throw new RuntimeException(s"$memory cannot be parsed. only bytes, kilobytes, megabytes, gigabytes, terabytes are supported")
    }
  }

}

object MemorySize {

  def apply(memory: String): MemorySize = new MemorySize(memory)

}
