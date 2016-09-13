package tech.artemisia.task.database.teradata.tpt

import tech.artemisia.task.database.teradata.{TeraUtils, DBInterfaceFactory}
import tech.artemisia.task.settings.DBConnection

/**
 * Created by chlr on 9/13/16.
 */

/**
 * TPT Script generator interface.
 *
 */
trait TPTScriptGenerator {

  protected val tptLoadConfig: TPTLoadConfig

  protected val loadSetting: TPTLoadSetting

  protected val dbConnection: DBConnection

  protected val dbInterface = DBInterfaceFactory.getInstance(dbConnection)

  protected lazy val tableMetadata = TeraUtils.tableMetadata(tptLoadConfig.databaseName, tptLoadConfig.tableName, dbInterface)

  protected val loadOperAtts: Map[String,(String, String)]

  protected val dataConnAttrs: Map[String,(String, String)]

  /**
   * tpt script
   */
   val tptScript: String

  protected def schemaDefinition = tableMetadata map { x => s""""${x._4}" VARCHAR(${x._3})""" } mkString "\n,"

  protected def loadOperatorAttributes = {
    loadOperAtts ++ loadSetting.loadOperatorAttrs map {
      case (attrName, (attrType, attrValue))
        if attrType.toUpperCase.trim == "VARCHAR" => s"VARCHAR $attrName = '$attrValue'"
      case (attrName, (attrType, attrValue)) => s"$attrType $attrName = $attrValue"
    } mkString ",\n"
  }

  protected def dataConnectorAttributes = {
    dataConnAttrs ++ loadSetting.dataConnectorAttrs map {
      case (attrName, (attrType, attrValue))
        if attrType.toUpperCase.trim == "VARCHAR" => s"VARCHAR $attrName = '$attrValue'"
      case (attrName, (attrType, attrValue)) => s"$attrType $attrName = $attrValue"
    } mkString ",\n"
  }

  def insertColumnList = tableMetadata map { x => s""""${x._1}""""} mkString "\n,"

  def valueColumnList = tableMetadata map { x => s":${x._4}"} mkString "\n,"

  def selectColumnList = {
    tableMetadata map { x => x._5.trim.toUpperCase flatMap {
      case 'N' => s""""${x._4}" as "${x._4}""""
      case _ =>
        s"""CASE WHEN "${x._4}" ='<!N!>' THEN NULL ELSE "${x._4}" END as "${x._4}"""".stripMargin
    }
    } mkString ",\n"
  }

}
