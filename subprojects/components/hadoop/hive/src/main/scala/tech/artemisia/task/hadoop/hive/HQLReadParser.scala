package tech.artemisia.task.hadoop.hive

import java.io.OutputStream

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import tech.artemisia.inventory.io.OutputLogParser

/**
  * Created by chlr on 8/6/16.
  */
class HQLReadParser(stream: OutputStream) extends OutputLogParser(stream) {

  var header: String = _
  var row: String = _
  var counter = 0

  override def parse(line: String): Unit = {
    counter match {
      case 0 => counter += 1; header = line
      case 1 => counter += 1; row = line
      case _ => ()
    }
  }

  def getData = {
    val joined = header.split('\t') zip row.split('\t')
    joined.foldLeft(ConfigFactory.empty()) {
      (carry, input) => carry.withValue(input._1, ConfigValueFactory.fromAnyRef(input._2))
    }
  }

}
