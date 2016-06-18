package tech.artemisia.task.database.mysql

import tech.artemisia.TestSpec
import tech.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 6/2/16.
  */
class DBInterfaceFactorySpec extends TestSpec {

  "DBInterfaceFactory" must "" in {
    DbInterfaceFactory.getInstance(DBConnection("dummy_dsn","","","",-1),"default") mustBe a
      [DbInterfaceFactory.DefaultDBInterface]

    DbInterfaceFactory.getInstance(DBConnection("dummy_dsn","","","",-1),"bulk") mustBe a
      [DbInterfaceFactory.NativeDBInterface]

  }

}
