package tech.artemisia.core

import tech.artemisia.TestSpec

/**
 * Created by chlr on 12/11/15.
 */
class CmdLineParsingSpec extends TestSpec {

  var arr: Array[String] = _

  override def beforeEach() = {
     arr  = Array("run","--location","/var/tmp","--run-id","5f40e8c3-59c2-4548-8e51-71980c657bc0",
      "--config","/home/user/config.yml","--context","k1=v1,k2=v2")
  }

  "CmdLineParser" must "parse cmd line arguments correctly" in {
    val cmd_line_args = Main.parseCmdLineArguments(arr)
    info("verifying cmd property")
    cmd_line_args.cmd.getOrElse("") must be ("run")
    info("verifying value property")
    cmd_line_args.value.getOrElse("") must be ("/var/tmp")
    info("verifying workflow_id property")
    cmd_line_args.run_id.getOrElse("") must be ("5f40e8c3-59c2-4548-8e51-71980c657bc0")
    info("verifying config property")
    cmd_line_args.config.getOrElse("") must be ("/home/user/config.yml")
    info("verifying context param")
    cmd_line_args.context.getOrElse("") must be ("k1=v1,k2=v2")
  }

  it must "throw an IllegalArgumentException when location parameter is missing for run command" in {
    arr(1) = "" ; arr(2) = "" //removing location argument and its value
    intercept[IllegalArgumentException] {
      Main.show_usage_on_error = false
      Main.main(arr)
    }
  }

}
