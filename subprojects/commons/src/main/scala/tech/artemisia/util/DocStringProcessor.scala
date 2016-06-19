package tech.artemisia.util

/**
 * Created by chlr on 6/18/16.
 */

object DocStringProcessor {

  implicit class StringUtil(content: String) {

    def ident(space: Int) = {
      val result: Seq[String] = content split System.lineSeparator map { " " * space + _ }
      result.head.trim :: (result.tail.toList map { x => if (x.startsWith("|")) s"|$x" else x }) mkString System.lineSeparator
    }

  }
}
