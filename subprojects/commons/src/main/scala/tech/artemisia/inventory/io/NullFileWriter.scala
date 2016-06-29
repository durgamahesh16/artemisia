package tech.artemisia.inventory.io

/**
 * Created by chlr on 5/15/16.
 */

/**
 * An empty implem
 */

class NullFileWriter extends FileDataWriter {

  override def writeRow(data: Array[String]): Unit = {
    totalRows += 1
  }

  override def writeRow(data: String): Unit = {
    totalRows += 1
  }

  override def close(): Unit = {}

  override var totalRows: Long = 0
}
