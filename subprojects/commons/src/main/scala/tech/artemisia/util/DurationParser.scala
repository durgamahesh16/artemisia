package tech.artemisia.util

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/**
  * Created by chlr on 7/30/16.
  */
class DurationParser(duration: String) {

  private val ns = "ns" :: "nano" :: "nanos" :: "nanosecond" :: "nanoseconds" :: Nil
  private val us = "us" :: "micro" :: "micros" :: "microsecond" :: "microseconds" :: Nil
  private val ms = "ms" :: "milli" :: "millis" :: "millisecond" :: "milliseconds" :: Nil
  private val se = "s" :: "second" :: "seconds" :: Nil
  private val mi = "m" :: "minute" :: "minutes" :: Nil
  private val hr = "h" :: "hour" :: "hours" :: Nil
  private val dy = "d" :: "day" :: "days" :: Nil


  def getFiniteDuration: FiniteDuration = {
    val patterns = ns ++ us ++ ms ++ se ++ mi ++ hr ++ dy
    patterns map { x => s"^([0-9]+)\\s*($x)$$".r } map {
      _.findFirstMatchIn(duration)
    } collect {
      case Some(x) => x.group(1) -> x.group(2)
    } match {
      case (value, unit) :: Nil if ns contains unit => FiniteDuration(value.toLong, TimeUnit.NANOSECONDS)
      case (value, unit) :: Nil if us contains unit => FiniteDuration(value.toLong, TimeUnit.MICROSECONDS)
      case (value, unit) :: Nil if ms contains unit => FiniteDuration(value.toLong, TimeUnit.MILLISECONDS)
      case (value, unit) :: Nil if se contains unit => FiniteDuration(value.toLong, TimeUnit.SECONDS)
      case (value, unit) :: Nil if mi contains unit => FiniteDuration(value.toLong, TimeUnit.MINUTES)
      case (value, unit) :: Nil if hr contains unit => FiniteDuration(value.toLong, TimeUnit.HOURS)
      case (value, unit) :: Nil if dy contains unit => FiniteDuration(value.toLong, TimeUnit.DAYS)
      case _ => throw new RuntimeException(s"$duration failed to be parsed as a duration")
    }
  }

}

object DurationParser {

  def apply(duration: String): DurationParser = new DurationParser(duration)

}


