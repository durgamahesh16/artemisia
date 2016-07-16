package tech.artemisia.task

import tech.artemisia.TestSpec

/**
  * Created by chlr on 7/15/16.
  */

class TestComponentSpec extends TestSpec {

  val testComponent = new TestComponent("Test")

  "Component" must "generate consolidate default config" in  {
    testComponent.consolidateDefaultConfig.getString("Test.TestAdderTask.ckey") must be ("cval")
    testComponent.consolidateDefaultConfig.getString("Test.TestFailTask.ckey") must be ("cval")
    testComponent.consolidateDefaultConfig.getString("Test.TestFailTask.tkey2") must be ("tval2")
    testComponent.consolidateDefaultConfig.getString("Test.TestAdderTask.tkey1") must be ("tval1")
  }
}
