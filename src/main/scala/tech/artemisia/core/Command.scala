package tech.artemisia.core

import tech.artemisia.inventory.exceptions.UnknownComponentException



/**
 * Created by chlr on 12/30/15.
 */
object Command {

  def run(cmd_line_params: AppSetting) = {
    val appContext = new AppContext(cmd_line_params)
    Runner.run(appContext)
  }

  def doc(cmdLineParam: AppSetting) = {
    val appContext = new AppContext(cmdLineParam)
    println(getDoc(appContext, cmdLineParam))
  }

  private[core] def getDoc(appContext: AppContext, cmdLineParam: AppSetting) = {
    cmdLineParam.component match {
      case Some(componentName) => {
        appContext.componentMapper.get(componentName) match {
          case Some(component) => cmdLineParam.task map { component.taskDoc } getOrElse component.doc
          case None => throw new UnknownComponentException(s"component ${cmdLineParam.component.get} doesn't exist")
        }
      }
      case None => appContext.componentMapper map { case(cName, cObj)  => s"$cName => ${cObj.info}" } mkString "\n"
    }
  }

}
