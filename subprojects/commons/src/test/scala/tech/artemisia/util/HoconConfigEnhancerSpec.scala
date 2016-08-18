package tech.artemisia.util

import com.typesafe.config.ConfigFactory
import HoconConfigUtil.configToConfigEnhancer
import tech.artemisia.TestSpec

/**
 * Created by chlr on 4/25/16.
 */
class HoconConfigEnhancerSpec extends TestSpec {

  "ConfigEnhancerSpec" must "resolve variable nested at arbitary depth" in {
    val testData = ConfigFactory parseString
      """
        |{
        |        "foo1": "bar1",
        |        "foo2": "bar2",
        |        "key1": "level1",
        |        "key2": [10, 20.9, {
        |                "key3": "level2",
        |                "key4": [{
        |                        "key5": "level3",
        |                        "key7": "${foo2}"
        |                }]
        |        }, "level1"],
        |        "key8": {
        |                "key9": {
        |                        "key10": "level3",
        |                        "key11": ["level4", ["level5", {
        |                                "key80": "${foo1}"
        |                        }]]
        |                }
        |        }
        |}
      """.stripMargin
    val resolvedConfig = testData.hardResolve()
    resolvedConfig.getAnyRefList("key2").get(2).asInstanceOf[java.util.Map[String, String]].get("key4")
      .asInstanceOf[java.util.List[AnyRef]].get(0).asInstanceOf[java.util.Map[String,String]]
      .get("key7") must be ("bar2")

    resolvedConfig.getConfig("key8").getConfig("key9").getAnyRefList("key11").get(1).asInstanceOf[java.util.List[AnyRef]]
      .get(1).asInstanceOf[java.util.Map[String,String]].get("key80") must be ("bar1")

  }


  it must "strip unnecessary leading space in quoted stings" in {
    val testData =
      """
        |  test1
        |    test2
      """.stripMargin
    val data = HoconConfigEnhancer.stripLeadingWhitespaces(testData)
    data.split("\n") map { _.length } must be (Seq(5,7))
  }

  it must "resolve variables which has dot character" in {
    val testData = ConfigFactory parseString
       """
        | {
        |   foo = {
        |      bar = baz
        |   }
        |   hello = ${foo.bar}
        | }
      """.stripMargin
    testData.hardResolve().getString("hello") must be ("baz")
  }

  it must "parse quoted strings properly" in {
    val testData = ConfigFactory parseString
      """
        | {
        |   foo.bar = baz
        |   hello = {
        |     world = [ { "foo.bar" = baz }, { "foo=bar"  = baz } ]
        |   }
        | }
      """.stripMargin
    val config = testData.hardResolve()
    config.getConfig("foo").getString("bar") must be ("baz")
    config.getConfig("hello").getConfigList("world").get(0).getString(""""foo.bar"""") must be ("baz")
    config.getConfig("hello").getConfigList("world").get(1).getString(""""foo=bar"""") must be ("baz")
  }

}
