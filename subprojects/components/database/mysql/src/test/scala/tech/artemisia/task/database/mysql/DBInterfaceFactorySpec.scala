package tech.artemisia.task.database.mysql

import tech.artemisia.TestSpec
import tech.artemisia.task.settings.ConnectionProfile

/**
  * Created by chlr on 6/2/16.
  */
class DBInterfaceFactorySpec extends TestSpec {

  "DBInterfaceFactory" must "" in {
    DbInterfaceFactory.getInstance(ConnectionProfile("dummy_dsn","","","",-1),"default") mustBe a
      [DbInterfaceFactory.DefualtDBInterface]

    DbInterfaceFactory.getInstance(ConnectionProfile("dummy_dsn","","","",-1),"native") mustBe a
      [DbInterfaceFactory.NativeDBInterface]

  }

}