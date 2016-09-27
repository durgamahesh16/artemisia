package tech.artemisia.util

import tech.artemisia.TestSpec
import tech.artemisia.util.BashUtil._
import tech.artemisia.util.FileSystemUtil._

/**
  * Created by chlr on 9/26/16.
  */
class BashUtilSpec extends TestSpec {


  "BashUtil" must "list files in directory" in {
    val path = joinPath(this.getClass.getResource("/arbitary/glob").getFile,"dir*","file*.txt")
    listFiles(path) must have length 4
  }

  it must "give size of file" in {
    val path = joinPath(this.getClass.getResource("/arbitary/glob").getFile,"dir*","file*.txt")
    pathSize(path).toInt must be > 0
  }

}
