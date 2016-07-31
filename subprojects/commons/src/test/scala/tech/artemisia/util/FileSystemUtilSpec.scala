package tech.artemisia.util

import java.io.{BufferedReader, InputStreamReader}
import java.nio.file.Paths

import tech.artemisia.TestSpec
/**
 * Created by chlr on 12/11/15.
 */
class FileSystemUtilSpec extends TestSpec  {

  "FileSystemUtil" must "Join multiple path strings into valid path string" in {
    var path1 = "/var/tmp"
    var path2 = "artemisia"
    var path3 = ""
    FileSystemUtil.joinPath (path1,path2,path3) must be ("/var/tmp/artemisia")
    path1 = "/var/tmp/"
    path2 = "/artemisia"
    FileSystemUtil.joinPath (path1,path2,path3) must be ("/var/tmp/artemisia")
    path1 = "/var/tmp"
    path2 = "/artemisia"
    FileSystemUtil.joinPath (path1,path2,path3) must be ("/var/tmp/artemisia")
    path1 = "var"
    path2 = "tmp"
    path3 = "artemisia"
    FileSystemUtil.joinPath (path1,path2,path3) must be ("/var/tmp/artemisia")
  }

  it must "properly construct URI object" in {
    val path1 = "/var/tmp/dir"
    val path2 = "hdfs://var/tmp/dir2"
    FileSystemUtil.makeURI(path1).toString must be ("file:/var/tmp/dir")
    FileSystemUtil.makeURI(path2).toString must be ("hdfs://var/tmp/dir2")
  }

  it must "it must resolve globs" in {
    val path = this.getClass.getClassLoader.getResource("arbitary/glob")
    val location = FileSystemUtil.joinPath(path.getFile ,"**/*.txt")
    val files = FileSystemUtil.expandPathToFiles(Paths.get(location))
    files must have size 4
    for (file <- files) { file.exists() mustBe true }
  }

  it must "reads all contents that points to multiple files via globbed path" in {
    val path = this.getClass.getClassLoader.getResource("arbitary/glob")
    val location = FileSystemUtil.joinPath(path.getFile ,"**/*.txt")
    val files = FileSystemUtil.expandPathToFiles(Paths.get(location))
    val reader = new BufferedReader(new InputStreamReader(FileSystemUtil.mergeFileStreams(files)))
    Stream.continually(reader.readLine()).takeWhile(_ != null).toArray must have size 8
  }

}
