package tech.artemisia.inventory.io

/**
 * Created by chlr on 5/15/16.
 */

abstract class FileDataWriter {

  var totalRows: Long

  def writeRow(data: Array[String])

  def writeRow(data: String)

  def close(): Unit

}

object FileDataWriter {

//  class NullFileWriter extends FileDataWriter {
//
//    override def writeRow(data: Array[String]): Unit = {
//      totalRows += 1
//    }
//
//    override def writeRow(data: String): Unit = {
//      totalRows += 1
//    }
//
//    override def close(): Unit = {}
//
//    override var totalRows: Long = 0
//  }

}
