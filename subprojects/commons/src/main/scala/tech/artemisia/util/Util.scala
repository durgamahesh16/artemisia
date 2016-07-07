package tech.artemisia.util

import java.io._
import java.nio.file.{Files, Paths}

import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import tech.artemisia.core.{AppLogger, Keywords}

/**
 * Created by chlr on 11/29/15.
 */


/**
 * an object with utility functions.
 *
 */
object Util {


  /**
    * get effective global config file
    * @param globalConfigFileRef config file set as environment variable
    * @return effective config file
    */
  def getGlobalConfigFile(globalConfigFileRef: Option[String], defaultConfig: String = Keywords.Config.DEFAULT_GLOBAL_CONFIG_FILE) = {
    globalConfigFileRef match {
      case Some(configFile) =>  Some(configFile)
      case None if Files exists Paths.get(defaultConfig) => Some(defaultConfig)
      case None => None
    }
  }

  /**
   *
   * @param path path of the HOCON file to be read
   * @return parsed Config object of the file
   */
  def readConfigFile(path: File): Config = {
    if(!path.exists()) {
      AppLogger error s"requested config file $path not found"
      throw new FileNotFoundException(s"The Config file $path is missing")
    }
    ConfigFactory parseFile path
  }


  /**
   * generates a UUID
   *
   * @return UUID
   */
  def getUUID = {
    java.util.UUID.randomUUID.toString
  }

  /**
   * prints stacktrace of an Exception
   *
   * @param ex Throwable object to be print
   * @return string of the stacktrace
   */
  def printStackTrace(ex: Throwable) = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    ex.printStackTrace(pw)
    sw.toString
  }

  /**
   **
   * @return current time in format "yyyy-MM-dd HH:mm:ss"
   */
  def currentTime : String = {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
      formatter.print(new DateTime())
  }

  def prettyPrintAsciiBanner(content: String, heading: String, width: Int = 80): String = {
   s"""
      |${"=" * (width / 2) } $heading ${"=" * (width / 2)}
      |$content
      |${"=" * (width / 2) } $heading ${"=" * (width / 2)}
    """.stripMargin
  }

  /**
    * An utlity function to generate markdown compatible ascii table for the two dimensional input
    *
    * {{{
    * val in = Array(Array("Country", "Captial"), Array("USA", "Washington"), Array("UK", "London"), Array("Russia", "Moscow"), Array("Japan", "Tokyo"))
    * print(prettyPrintAsciiTable(in))
    *
    * | Country  | Capital     |
    * | ---------| ------------|
    * | USA      | Washington  |
    * | UK       | London      |
    * | Russia   | Moscow      |
    * | Japan    | Tokyo       |
    * }}}
    *
    * @param content two dimensional array representation of the table
    * @return content in ascii table string
    */
  def prettyPrintAsciiTable(content: Array[Array[String]]) = {

    val tableDimensions = content.foldLeft(for(i <- 1 to content(0).length) yield 0 ) {
      (carry , input) => {
        carry zip input map ( x => x._1.max(x._2.length) )
      }
    } map { _ + 2 }

    def composeRow(row: Array[String], divider: Boolean = false) = { row zip tableDimensions map {
      x => s"|${if(divider)"-" else " "}${x._1}${" " * (x._2 - x._1.length)}" } mkString ""
    }

    content.toList match {
      case head :: Nil => composeRow(head) :: composeRow(tableDimensions.map("-"* _ ).toArray) :: Nil
      case head :: tail => {
        val x = composeRow(head) :: composeRow(tableDimensions.map("-"* _ ).toArray, divider = true) :: tail.map(composeRow(_))
        (x mkString "|\n") + "|"
      }
      case Nil => throw new RuntimeException("content cannot be empty")
    }

  }


}
