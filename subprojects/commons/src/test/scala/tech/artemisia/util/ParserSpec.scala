package tech.artemisia.util

import java.util.concurrent.TimeUnit

import tech.artemisia.TestSpec

import scala.concurrent.duration.FiniteDuration

/**
  * Created by chlr on 7/31/16.
  */
class ParserSpec extends TestSpec {

  "MemoryParser" must "parse memory string literal" in {

    MemorySize("10KiB").toBytes must be (10240L)
    MemorySize("10 kilobyte").toBytes must be (10240L)
    MemorySize("10kB").toBytes must be (10240L)
    MemorySize("10k").toBytes must be (10240L)


    MemorySize("10m").toBytes must be (10485760L)
    MemorySize("10M").toBytes must be (10485760L)
    MemorySize("10MB").toBytes must be (10485760L)
    MemorySize("10MiB").toBytes must be (10485760L)
    MemorySize("10 megabyte").toBytes must be (10485760L)

    MemorySize("10 byte").toBytes must be (10L)
    MemorySize("10B").toBytes must be (10L)
    MemorySize("10b").toBytes must be (10L)


    MemorySize("9G").toBytes must be (9663676416L)
    MemorySize("9g").toBytes must be (9663676416L)
    MemorySize("9GiB").toBytes must be (9663676416L)
    MemorySize("9 gigabyte").toBytes must be (9663676416L)

    MemorySize("9T").toBytes must be (9895604649984L)
    MemorySize("9Ti").toBytes must be (9895604649984L)
    MemorySize("9TiB").toBytes must be (9895604649984L)
    MemorySize("9 terabyte").toBytes must be (9895604649984L)

  }


  "DurationParser" must "parse duration literal" in {
    DurationParser("10ns").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.NANOSECONDS))
    DurationParser("10 nanosecond").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.NANOSECONDS))
    DurationParser("10 nanos").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.NANOSECONDS))

    DurationParser("10us").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MICROSECONDS))
    DurationParser("10 micro").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MICROSECONDS))
    DurationParser("10 microsecond").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MICROSECONDS))

    DurationParser("10 milli").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MILLISECONDS))
    DurationParser("10ms").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MILLISECONDS))
    DurationParser("10 millisecond").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MILLISECONDS))

    DurationParser("9 second").toFiniteDuration must be (FiniteDuration(9L, TimeUnit.SECONDS))
    DurationParser("9s").toFiniteDuration must be (FiniteDuration(9L, TimeUnit.SECONDS))

    DurationParser("10m").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MINUTES))
    DurationParser("10 minute").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MINUTES))

    DurationParser("100h").toFiniteDuration must be (FiniteDuration(100L, TimeUnit.HOURS))
    DurationParser("100 hours").toFiniteDuration must be (FiniteDuration(100L, TimeUnit.HOURS))

    DurationParser("98d").toFiniteDuration must be (FiniteDuration(98L, TimeUnit.DAYS))
    DurationParser("98 days").toFiniteDuration must be (FiniteDuration(98L, TimeUnit.DAYS))


  }

}
