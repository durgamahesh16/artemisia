package tech.artemisia.task.database.teradata

import tech.artemisia.TestSpec
import tech.artemisia.task.database.TestDBInterFactory

/**
  * Created by chlr on 9/16/16.
  */
class TeraUtilSpec extends TestSpec {

  "TeraUtil" must "retrieve table column metadata" in {
    implicit val dbInterface = TestDBInterFactory.withDefaultDataLoader("Columns", database = "dbc", createTestTable = false)
    dbInterface.execute("CREATE SCHEMA DBC;")
    dbInterface.execute(
      """
        |CREATE TABLE dbc.Columns
        |(
        | ColumnId INT,
        | ColumnName varchar(30),
        | Tablename varchar(30),
        | Databasename varchar(30),
        | ColumnType varchar(5),
        | decimaltotaldigits INT,
        | ColumnLength int,
        | Nullable char(1)
        |);
      """.stripMargin)
    dbInterface.execute("INSERT INTO dbc.Columns VALUES (1, 'col1', 'tablename', 'databasename', 'DA', 10, 21,'Y')")
    dbInterface.execute("INSERT INTO dbc.Columns VALUES (2, 'col2', 'tablename', 'databasename', 'DA', 10, 21,'Y')")
    val metadata = TeraUtils.tableMetadata("databasename", "tablename")
    metadata must contain theSameElementsInOrderAs Seq(("col1", "DA", 15, "col1_1_", "Y"),("col2","DA",15,"col2_2_","Y"))
  }

}

