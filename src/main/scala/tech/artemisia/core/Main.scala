package tech.artemisia.core

import scopt.OptionParser

object Main {

  var show_usage_on_error = true

  def main(args: Array[String]): Unit = {
    Thread.currentThread().setName(Keywords.APP)
    parseCmdLineArguments(args,show_usage_on_error) match {
      case cmdLineParams @ AppSetting(Some("run"), Some(_), _, _, _, _,_,_,_) => Command.run(cmdLineParams)
      case cmdLineParams @ AppSetting(Some("doc"), _, _, _, _, _,_,component,task) => Command.doc(cmdLineParams)
      case cmdLineParams @ _ => {
        println(cmdLineParams)
        throw new IllegalArgumentException("--help to see supported options")
         }
      }
    }

  private[core] def parseCmdLineArguments(args: Array[String],usageOnError: Boolean = true): AppSetting = {

    val parser = new OptionParser[AppSetting](Keywords.APP) {
      head(Keywords.APP)
        arg[String]("cmd") action { (x, c) =>  c.copy(cmd = Some(x)) } required() children {
        opt[String]('l', "location")  action { (x, c) => c.copy(value = Some(x)) } text "location of the job conf"
        opt[String]('w',"workdir") action { (x,c) => c.copy( working_dir = Some(x) ) } text "set the working directory for the current job"
        opt[Unit]('n',"no-checkpoint") action { (x,c) => c.copy(skip_checkpoints = true) } text "set this property skip checkpoints"
        opt[String]('r', "run-id") action { (x, c) => c.copy(run_id = Some(x)) } text "run_id for execution"
        opt[String]('c', "component") action { (x,c) => c.copy(component = Some(x)) }
        opt[String]('t', "task") action { (x, c) => c.copy(task = Some(x)) }
      } text "command options"
      opt[String]("context") valueName "k1=v1,k2=v2..." action { (x, c) => c.copy(context = Some(x)) }
      opt[String]("config") action { (x, c) => c.copy(config = Some(x)) } text "configuration file"

      override def showUsageOnError = usageOnError

      override def errorOnUnknownArgument = true
    }

    parser.parse(args, AppSetting()).getOrElse(AppSetting())

  }
}

