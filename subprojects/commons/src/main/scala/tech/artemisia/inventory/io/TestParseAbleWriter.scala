package tech.artemisia.inventory.io

import scala.collection.mutable

/**
 * Created by chlr on 8/3/16.
 */
class TestParseAbleWriter extends ParseableWriter {

  val data =  mutable.Buffer[String]()

  override def parse(lines: Seq[String]): Unit = {
    lines filter { _.startsWith("_") } foreach {
      x => data += x
    }
  }

}
