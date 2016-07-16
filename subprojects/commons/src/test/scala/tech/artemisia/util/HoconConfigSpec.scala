package tech.artemisia.util

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import HoconConfigUtil.Handler
import tech.artemisia.TestSpec

import scala.concurrent.duration.FiniteDuration

/**
 * Created by chlr on 4/16/16.
 */
class HoconConfigSpec extends TestSpec {

  val config = ConfigFactory parseString
  """
    |{
    |	"int" : 100
    | "intList" : [101, 102]
    | "long" = 10924236432
    | "longList" = [1233243428273,1233243428274]
    |	"string": "foxtrot"
    | "stringList" = ["whiskey", "tango", "foxtrot"]
    |	"boolean": yes
    |	"booleanList": [yes, no]
    |	"config" : {
    |				"field1" : "value1"
    |				"field3" : "value2"
    |	  }
    |	"configList" : [{ "tango1" : "whiskey1" }, { "tango2" : "whiskey2" } ]
    |	"double" : 10.32
    |	"doubleList" : [12.23, 827.213]
    |	"duration" : 30m
    |	"durationList" : [34m, 34h],
    | "char" : ",",
    | "encodedchar" : "\\u0001"
    |
    |}
    |
  """.stripMargin

  "HoconConfig" must "parse Integers fields correctly" in {
    config.as[Int]("int") must be (100)
  }

  it must "parse list of int fields correctly" in {
    val data = config.as[List[Int]]("intList")
    data(1) must be (102)
  }

  it must "parse String fields correctly" in {
    config.as[String]("string") must be ("foxtrot")
  }

  it must "parse list of String fields correctly" in {
    config.as[List[String]]("stringList") must be (Seq("whiskey", "tango", "foxtrot"))
  }

  it must "parse long fields correctly" in {
    config.as[Long]("long") must be (10924236432L)
  }

  it must "parse list of long fields correctly" in {
    config.as[List[Long]]("longList").head must be (1233243428273L)
  }

  it must "parse boolean fields correctly" in {
    config.as[Boolean]("boolean") must be (right = true)
  }

  it must "parse list of boolean fields correctly" in {
    config.as[List[Boolean]]("booleanList") must be (Seq(true, false))
  }

  it must "parse object fields correctly" in {
    val data = config.as[Config]("config")
    data.as[String]("field1") must be ("value1")
  }

  it must "parse list of object fields correctly" in {
    val data = config.as[List[Config]]("configList")
    data.head.as[String]("tango1") must be ("whiskey1")
  }

  it must "parse double fields correctly" in {
    config.as[Double]("double") must be (10.32)
  }

  it must "parse list of double fields correctly" in {
    config.as[List[Double]]("doubleList") must be (Seq(12.23, 827.213))
  }

  it must "parse duration fields correctly" in {
    config.as[FiniteDuration]("duration") must be (FiniteDuration(30, unit=TimeUnit.MINUTES))
  }

  it must "parse list of duration fields correctly" in {
    val data = config.as[List[FiniteDuration]]("durationList")
    data(1) must be (FiniteDuration(34, unit=TimeUnit.HOURS))
  }

  it must "parse character fields correctly" in {
    config.as[Char]("char") must be (',')
  }

  it must "parse encoded characters correctly" in {
    config.as[Char]("encodedchar") must be ('\u0001')
  }

  it must "render Config Objects" in {
    val config = ConfigFactory parseString
      s"""
         | {
         |   goku = vegeta
         |   avengers = [ ironman, thor, hulk, capt america, black widow ]
         |   justice_league = {
         |              batman = bruce
         |              superman = clark
         |              wonderwoman = gal gadot
         |              others = [supergirl, martian, green latern]
         |           }
         | }
       """.stripMargin
    val reConfig = ConfigFactory parseString  HoconConfigUtil.render(config.root())

   reConfig.root().render(ConfigRenderOptions.concise()) must be (config.root().render(ConfigRenderOptions.concise()))

  }



}
