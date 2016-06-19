package tech.artemisia.task.localhost

import com.typesafe.config.{ConfigRenderOptions, ConfigFactory}
import tech.artemisia.TestSpec

/**
 * Created by chlr on 6/18/16.
 */
class CoreComponentSpec extends TestSpec {

  val component = new CoreComponent("MySQL")

  "CoreComponent" must "dispatch ScriptTask when requested" in {
      val config = ConfigFactory parseString
        """
          |{
          |  script = "echo Hello World"
          |  interpreter = /usr/local/bin/sh
          |  cwd = /var/tmp
          |  env = { foo = bar, hello = world }
          |  parse_output = no
          |}
        """.stripMargin
      val task = component.dispatchTask(ScriptTask.taskName, "script", config)
      task mustBe a [ScriptTask]
  }

  it must "dispatch EmailTask when requested" in {
    val config = ConfigFactory parseString
      s"""
        |{
        |  connection = ${EmailTaskSpec.defaultConnectionConfig.root().render()}
        |  email = ${EmailTaskSpec.defaultEmailRequestConfig.root().render()}
        |}
      """.stripMargin
    val task = component.dispatchTask(EmailTask.taskName, "email", config)
    task mustBe a [EmailTask]
  }
}
