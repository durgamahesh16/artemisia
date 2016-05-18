package tech.artemesia.util

import java.io._
import java.net.URI
import tech.artemesia.task.TaskContext

import scala.io.Source

/**
 * Created by chlr on 3/6/16.
 */




/**
 * An aggregation of Helper methods related to FileSystem based operations
 *
 */
object FileSystemUtil {

  /**
   * read a text-file using the classloader
   *
   * @param resource name of the resource to be read.
   * @return String content of the resource
   */
  def readResource(resource: String) = {
    val resource_stream = this.getClass.getResourceAsStream(resource)
    val buffered = new BufferedReader(new InputStreamReader(resource_stream))
    buffered.lines().toArray.mkString("\n")
  }

  /**
   * write the given content to text-file
   *
   * @param content content to be written
   * @param file path of the file
   * @param append set true to append content to the file.
   */
  def writeFile(content: String,file: File, append: Boolean = true) {
    file.getParentFile.mkdirs()
    val writer = new BufferedWriter(new FileWriter(file,append))
    writer.write(content)
    writer.close()
  }


  /**
   * returns a composed path joining input paths
   *
   * {{{
     *   joinPath("/home","/artemesia/","mydirectory")
   * }}}
   *
   * would return /home/artemesia/mydirectory
   *
   * @param path varags of paths to be joined
   * @return new path
   */
  def joinPath(path: String*) = {
    path.foldLeft(System.getProperty("file.separator"))((a: String, b: String) => new File(a, b).toString)
  }

  /**
   * build URI object from the given path. set file as the default scheme
   * @param path path to be converted
   * @return java.net.URI object
   */
  def makeURI(path: String) = {
    if (URI.create(path).getScheme == null)
       new File(path).toURI
    else
      new URI(path)
  }

  /**
   * an utility function that works similar to try with resources
   * @param fileName name of the file to be created
   * @param body the code block that takes the newly created file as input
   * @return
   */
  def withFile(fileName: String)(body: File => Unit): Unit = {
    val file = new File(TaskContext.workingDir.toFile,fileName)
    body(file)
    file.delete()
  }


  /**
   * A pimp my library pattern for enhancing java.io.File object with BufferedWriter methods
   * @param file file object to be enhanced.
   */
  implicit class FileEnhancer(file: File) {

    private val writer = new BufferedWriter(new FileWriter(file))
    private val reader = new BufferedReader(new FileReader(file))

    /**
     * write provided content appended with a new line
     * @param content
     */
    def <<= (content: String) = {
      assert(file.isFile, "cannot write to a directory")
      writer.write(content+"\n")
      writer.flush()
    }

    /**
     *
     * @return content of the file
     */
    def content = {
      assert(file.isFile, "cannot write to a directory")
      Source.fromFile(file).getLines().mkString("\n")
    }

  }

}
