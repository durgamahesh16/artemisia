package tech.artemisia.task.hadoop

import java.io._
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.Progressable


/**
  * Created by chlr on 7/17/16.
  */


object HDFSUtil {

  /**
    * create an InputStream for the given URI
    * @param uri URI to read from
    * @return inputstream for the URI
    */
  def readIOStream(uri: URI) = {
    val fileSystem = FileSystem.get(uri, new Configuration())
    fileSystem.open(new Path(uri))
  }

  /**
    * create a data outputstream to write to the given URI
    * @param uri uri to write to
    * @param overwrite set true to overwrite file if already exists.
    * @param bufferSize buffer size for the stream
    * @param replication replication factor for the file.
    * @param blockSize hdfs block size for the target file
    * @return outputstream for the input URI
    */
  def writeIOStream(uri: URI, overwrite: Boolean = false, bufferSize: Int, replication: Short = 3, blockSize: Int) = {
    val fileSystem = FileSystem.get(uri, new Configuration())
    fileSystem.create(new Path(uri), new FsPermission(644: Short)
      , overwrite
      , bufferSize
      , replication
      , blockSize
      , new Progressable {
        override def progress(): Unit = {}
      }
    )
  }

  /**
    * resolve a path with glob patterns to a list of files
    * @param uri uri of the path to be resolved
    * @param filesOnly allow only files in the result and filter any directories
    * @return list of files/directories resolved from URI.
    */
  def expandPath(uri: URI, filesOnly: Boolean = true) = {
    val fileSystem = FileSystem.get(uri, new Configuration())
    val paths = fileSystem.globStatus(new Path(uri))
    paths filter {
      !filesOnly || ! _.isFile
    } map {
      _.getPath
    }
  }

  /**
    * merge multiple files into a single IOStream
    * @param files list of the files in HDFS
    * @return a single IOStream
    */
  def mergeFileIOStreams(files: Seq[Path]) = {
    val fileSystem = FileSystem.get(new URI(files.head.toString), new Configuration())
    val identity: InputStream = new ByteArrayInputStream(Array[Byte]())
    files.foldLeft(identity) {
      (carry: InputStream, input: Path) =>
          new SequenceInputStream(carry, fileSystem.open(input))
    }
  }


}
