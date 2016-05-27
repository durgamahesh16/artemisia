package tech.artemisia.core

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec

/**
 * Created by chlr on 5/26/16.
 */
class BooleanEvaluatorSpec extends TestSpec {

  "BooleanEvaluator" must "parse expression accurately" in {

    val stringExpr = " 1 == 0 "
    BooleanEvaluator evalBooleanExpr stringExpr mustBe false

    val arrExpr1 = ConfigFactory parseString s"""foo = [ "1 == 1", "2 == 2", "3 == 2" ]"""
    BooleanEvaluator evalBooleanExpr arrExpr1.getAnyRefList("foo") mustBe false

    val arrExpr2 = ConfigFactory parseString s"""foo = [ "1 == 1", "2 == 2", " 3 == 3" ]"""
    BooleanEvaluator evalBooleanExpr arrExpr2.getAnyRefList("foo") mustBe true

    val objExpr1 = ConfigFactory parseString
      """
        | foo = {
        |  or = [ "1 == 0", " 1 == 1" ]
        |  and = [ "1 == 1", " 2 == 2 " ]
        |}
      """.stripMargin
    BooleanEvaluator evalBooleanExpr objExpr1.getConfig("foo").root().unwrapped() mustBe true


    val objExpr2 = ConfigFactory parseString
      """
        | foo = {
        |  or = [ "1 == 0", " 1 == 1" ]
        |  and = [ "1 == 1", " 2 == 1" ]
        |}
      """.stripMargin
    BooleanEvaluator evalBooleanExpr objExpr2.getConfig("foo").root().unwrapped() mustBe false

  }


}
