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
