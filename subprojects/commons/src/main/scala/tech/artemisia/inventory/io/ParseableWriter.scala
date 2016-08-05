package tech.artemisia.inventory.io


import java.io.Writer
import scala.collection.mutable

/**
  * Created by chlr on 8/3/16.
  */

/**
  *
  */
abstract class ParseableWriter extends Writer {

  val buffer = mutable.Buffer[Char]()


  final override def flush(): Unit = {
    val content = getLines()
    parse(content)
    print(content mkString System.lineSeparator())
  }

  final override def write(cbuf: Array[Char], off: Int, len: Int): Unit = {
    buffer ++= cbuf.slice(off, len)
  }

  final override def close(): Unit = {
    val content = buffer mkString ""
    parse(content split System.lineSeparator())
    print(content)
  }

  final def getLines() = {
    buffer.mkString("").split(System.lineSeparator()).toSeq match {
      case init :+ last => {
        buffer.clear()
        buffer ++= last
        init
      }
      case head :: Nil =>  Seq()
      case Nil => Seq()
    }
  }

  def parse(lines: Seq[String]): Unit

}