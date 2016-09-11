package tech.artemisia.task.database.teradata

/**
  * Created by chlr on 9/8/16.
  */

/**
  *
  * @param databaseName
  * @param tableName
  * @param directory
  * @param fileName
  */
case class TPTLoadConfig(databaseName: String, tableName: String, directory: String, fileName: String)
