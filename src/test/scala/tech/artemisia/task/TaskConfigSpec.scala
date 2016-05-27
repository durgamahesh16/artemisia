package tech.artemisia.task

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec
import tech.artemisia.core.BooleanEvaluator

/**
 * Created by chlr on 5/26/16.
 */
class TaskConfigSpec extends TestSpec {

  "TaskConfig" must "parse description and expression nodes properly" in {
    val config = ConfigFactory parseString
      s""" foo = {
        |  ${BooleanEvaluator.description} = "This is a description node"
        |  ${BooleanEvaluator.expression} = "1 == 1"
        |} """.stripMargin

    val (bool: Boolean, desc: String) = TaskConfig.parseConditionsNode(config.getConfig("foo").root())
    bool mustBe true
    desc mustBe "This is a description node"
  }

  it must "parse node with description and expression" in {

    val config = ConfigFactory parseString
      """
        | foo =  "100 == 1000"
      """.stripMargin
    val (bool: Boolean, desc: String) = TaskConfig.parseConditionsNode(config.getValue("foo"))
    bool mustBe false
    desc mustBe "100 == 1000"
  }


}
