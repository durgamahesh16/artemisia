package tech.artemisia.core

import tech.artemisia.TestSpec
/**
  * Created by chlr on 6/6/16.
  */
class TaskDocSpec extends TestSpec {

  "Command" must "fetch Component doc when requested" in {
      val appSetting = AppSetting(cmd = Some("doc"), component = Some("TestComponent"), task = None)
      val appContext = new AppContext(appSetting)
      val result = Command.getDoc(appContext, appSetting)
      result must be("this is TestComponent doc")
  }

  it must "fetch task doc when requested" in {
      val appSetting = AppSetting(cmd = Some("doc"), component = Some("TestComponent"), task = Some("TestAdderTask"))
      val appContext = new AppContext(appSetting)
      val result = Command.getDoc(appContext, appSetting)
      result must be("TestAdderTask is a test addition task")
  }

  it must "must fetch component list" in {
      val appSetting = AppSetting(cmd = Some("doc"), component = None, task = None)
      val appContext = new AppContext(appSetting)
      val result = Command.getDoc(appContext, appSetting)
      result must be("TestComponent => This is a TestComponent")
  }

}
