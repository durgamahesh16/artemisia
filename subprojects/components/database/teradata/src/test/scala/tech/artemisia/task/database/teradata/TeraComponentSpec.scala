package tech.artemisia.task.database.teradata

import tech.artemisia.TestSpec

/**
 * Created by chlr on 7/12/16.
 */
class TeraComponentSpec extends  TestSpec {

  "TeraComponent" must "assign right defaults" in {
    val component = new TeraComponent("Teradata")
    component.tasks must have length 10
    component.defaultConfig.getInt("dsn.port") must be (1025)
  }

}
