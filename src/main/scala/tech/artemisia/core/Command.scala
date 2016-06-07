package tech.artemisia.core

import tech.artemisia.inventory.exceptions.UnknownComponentException
import tech.artemisia.task.Component



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
    appContext.componentMapper.get(cmdLineParam.component.get) match {
      case Some(component) => println(getDoc(component, cmdLineParam.task))
      case None => throw new UnknownComponentException(s"component ${cmdLineParam.component.get} doesn't exist")
    }

  }

  private def getDoc(component: Component, task: Option[String]) = {
      task match {
      case Some(taskName) => component.taskDoc(taskName)
      case None => component.doc
    }
  }

}
