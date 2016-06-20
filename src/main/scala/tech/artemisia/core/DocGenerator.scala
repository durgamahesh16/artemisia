package tech.artemisia.core

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Path, Paths}

import scala.collection.JavaConverters._
import org.apache.commons.io.FileUtils
import org.yaml.snakeyaml.Yaml
import tech.artemisia.task.Component
import tech.artemisia.util.FileSystemUtil

import scala.collection.mutable

/**
 * Created by chlr on 6/19/16.
 */
object DocGenerator {

   var baseDir: Path = _

  def main(args: Array[String]) = {
    baseDir = Paths.get(args(0))
    FileUtils.deleteDirectory(new File(FileSystemUtil.joinPath(baseDir.toString, "docs", "components")))
    getComponents foreach { case(a,b) => writeComponentDoc(a,b) }
    generateMkDocConfig(getComponents.toList map {_._1})
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

  private def generateMkDocConfig(components: Seq[String]) = {
    val yaml = new Yaml()
    val config: mutable.Map[String, Object] = yaml.load(mkDocConfigFile).asInstanceOf[java.util.Map[String,Object]].asScala
    val componentConfig = components.map(x => Map(x -> s"components/${x.toLowerCase}.md").asJava ).toSeq.asJava
    config("pages").asInstanceOf[java.util.List[Object]].add(Map("Components" -> componentConfig).asJava)
    val writer = new BufferedWriter(new FileWriter(new File(FileSystemUtil.joinPath(baseDir.toString, "mkdocs.yml"))))
    yaml.dump(config.asJava, writer)
  }

  private def mkDocConfigFile = {
   s"""
      |site_name: Artemisia
      |theme: readthedocs
      |pages:
      |   - Home: index.md
      |   - Getting Started:
      |      - Installation: started.md
      |      - Defining Jobs: concept.md
    """.stripMargin
  }



}
