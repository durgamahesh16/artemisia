package tech.artemisia.util

import java.io.{File, FileNotFoundException}

import tech.artemisia.TestSpec


/**
 * Created by chlr on 1/1/16.
 */

class UtilSpec extends TestSpec {

  "The Util.readConfigFile" must "throw FileNotFoundException on non-existent file" in {
    intercept[FileNotFoundException] {
      Util.readConfigFile(new File("Some_Non_Existant_File.conf"))
    }
  }

  it must "give back global config file is explicitly set" in {
    FileSystemUtil.withTempFile(fileName = "global_file.txt") {
      file => {
        Util.getGlobalConfigFile(Some(file.toPath.toString)) must be (Some(file.toPath.toString))
      }
    }
  }


  it must "give back default config file when global is not set" in {
    FileSystemUtil.withTempFile(fileName = "global_file.txt") {
      file => {
        Util.getGlobalConfigFile(None, defaultConfig = file.toPath.toString) must be (Some(file.toPath.toString))
      }
    }
  }

  it must "give back None when global config is not set and default doesn't exists" in {
     Util.getGlobalConfigFile(None, defaultConfig =  "a_dummy_non_existant_file") must be (None)
  }


}
