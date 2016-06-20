package tech.artemisia.core

import java.io.File
import java.nio.file.{Path, Paths}

import org.apache.commons.io.FileUtils
import tech.artemisia.task.Component
import tech.artemisia.util.FileSystemUtil

/**
 * Created by chlr on 6/19/16.
 */
object DocGenerator {

   var baseDir: Path = _

  def main(args: Array[String]) = {
    baseDir = Paths.get(args(0))
    FileUtils.deleteDirectory(new File(FileSystemUtil.joinPath(baseDir.toString, "docs", "components")))
    getComponents foreach { case(a,b) => writeComponentDoc(a,b) }
  }

  private def getComponents = {
    val appSetting = AppSetting(cmd = Some("doc"))
    new AppContext(appSetting).componentMapper
  }

  private def writeComponentDoc(componentName: String, component: Component) = {
    val fileName = s"${componentName.toLowerCase}.md"
    val filePath = FileSystemUtil.joinPath(baseDir.toString, "docs", "components", fileName)
    FileSystemUtil.writeFile(componentDoc(component), new File(filePath))
  }

  private def componentDoc(component: Component) = {

    s"""
       ! ${component.doc}
       !
       ! ${component.tasks map { _.doc(component.name) } mkString (System.lineSeparator * 4) }
       !
     """.stripMargin('!')

  }



}
