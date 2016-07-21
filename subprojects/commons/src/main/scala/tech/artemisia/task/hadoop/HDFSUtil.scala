package tech.artemisia.task.hadoop

import java.io._
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.util.Progressable


/**
  * This Object houses assortments of HDFS based utility functions.
  * The following compression formats are supported both for reading and writing.
  *  - default
  *  - bzip2
  *  - gzip
  */
object HDFSUtil {

  /**
    *
    * @param uri input uri to read
    * @param codec Option of compression codec to be used
    * @return InputStream of the input URI
    */
  def readIOStream(uri: URI, codec: Option[String] = None ) = {
    val path = new Path(uri)
    val fileSystem = FileSystem.get(uri, new Configuration())
    getEffectiveCodec(codec, path) map { x => x.createInputStream(fileSystem.open(path)) } getOrElse fileSystem.open(path)

  }

  /**
    * create a data outputstream to write to the given URI
    *
    * @param uri uri to write to
    * @param overwrite set true to overwrite file if already exists.
    * @param replication replication factor for the file.
    * @param blockSize hdfs block size for the target file
    * @return outputstream for the input URI
    */

  def writeIOStream(uri: URI, overwrite: Boolean = false, replication: Short = 3, blockSize: Long
                   ,codec: Option[String] = None) = {
    val path = new Path(uri)
    val fileSystem = FileSystem.get(uri, new Configuration())
    val stream = fileSystem.create(new Path(uri), new FsPermission(644: Short)
      , overwrite
      , 62914560 // should buffer size be configurable by the user?
      , replication
      , blockSize
      , new Progressable {
        override def progress(): Unit = {}
      }
    )
    getEffectiveCodec(codec, path) map { x => x.createOutputStream(stream) } getOrElse stream
  }

  /**
    * resolve a path with glob patterns to a list of files
    *
    * @param uri uri of the path to be resolved
    * @param filesOnly allow only files in the result and filter any directories
    * @return list of files/directories resolved from URI.
    */
  def expandPath(uri: URI, filesOnly: Boolean = true) = {
    val fileSystem = FileSystem.get(uri, new Configuration())
    val paths = fileSystem.globStatus(new Path(uri))
    paths filter {
      !filesOnly ||  _.isFile
    } map {
      _.getPath
    }
  }

  /**
    * returns the option of compression codec to be used.
    * Its takes an Option of compression codec explicitly specified by the user (read explicitCodec)
    * and implicit compressionCodec detected from the file extension. The explicit compression codec
    * takes precedence over implicit one
    * @param explicitCodec Option of explicit compression codec
    * @param path path object to detect implicit compression codec
    * @return Option of effective compression codec to be used.
    */
  private[hadoop] def getEffectiveCodec(explicitCodec: Option[String], path: Path) = {
    val factory = new CompressionCodecFactory(new Configuration())
    (explicitCodec.map(factory.getCodecByName) ++  Option(factory.getCodec(path))).headOption
  }

  /**
    * merge multiple files into a single IOStream
    *
    * @param files list of the files in HDFS
    * @return a single IOStream
    */
  def mergeFileIOStreams(files: Seq[Path], codec: Option[String] = None) = {
    val identity: InputStream = new ByteArrayInputStream(Array[Byte]())
    files.foldLeft(identity) {
      (carry: InputStream, input: Path) =>
          new SequenceInputStream(carry, HDFSUtil.readIOStream(input.toUri,codec))
    }
  }


}
