package tech.artemisia.core

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec

/**
  * Created by chlr on 5/26/16.
  */
class BooleanEvaluatorSpec extends TestSpec {

  val stringExpr = " 1 == 0 "
  val arrExpr1 = ConfigFactory parseString s"""foo = [ "1 == 1", "2 == 2", "3 == 2" ]"""
  val arrExpr2 = ConfigFactory parseString s"""foo = [ "1 == 1", "2 == 2", " 3 == 3" ]"""
  val objExpr1 = ConfigFactory parseString
    """
      | foo = {
      |  or = [ "1 == 0", " 1 == 1" ]
      |  and = [ "1 == 1", " 2 == 2 " ]
      |}
    """.stripMargin
  val objExpr2 = ConfigFactory parseString
    """
      | foo = {
      |  or = [ "1 == 0", " 1 == 1" ]
      |  and = [ "1 == 1", " 2 == 1" ]
      |}
    """.stripMargin


  "BooleanEvaluator" must "parse expression accurately" in {

    BooleanEvaluator evalBooleanExpr stringExpr mustBe false
    BooleanEvaluator evalBooleanExpr arrExpr1.getAnyRefList("foo") mustBe false
    BooleanEvaluator evalBooleanExpr arrExpr2.getAnyRefList("foo") mustBe true
    BooleanEvaluator evalBooleanExpr objExpr1.getConfig("foo").root().unwrapped() mustBe true
    BooleanEvaluator evalBooleanExpr objExpr2.getConfig("foo").root().unwrapped() mustBe false
    BooleanEvaluator evalBooleanExpr true mustBe true
    BooleanEvaluator evalBooleanExpr false mustBe false
    BooleanEvaluator evalBooleanExpr objExpr1.getValue("foo") mustBe true
    BooleanEvaluator evalBooleanExpr objExpr2.getValue("foo") mustBe false

  }


}
