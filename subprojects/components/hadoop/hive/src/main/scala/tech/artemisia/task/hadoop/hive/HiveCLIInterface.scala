package tech.artemisia.task.hadoop.hive

import tech.artemisia.core.AppLogger._
import tech.artemisia.task.TaskContext
import tech.artemisia.util.CommandUtil._
import tech.artemisia.util.FileSystemUtil._
import tech.artemisia.util.Util

/**
  * Created by chlr on 8/3/16.
  */

/**
  * A DBInterface like class that supports functionality like queryOne and execute
  * but doesnt extends DBInterace. The Local Hive CLI installation is used to
  * submit queries. This interface can be used in the absence of a hiveserver service
  * not being available.
  */
class HiveCLIInterface {


  /**
    *
    * @param hql SELECT query to be executed.
    * @param taskName map reduce job name to be set for the HQL query.
    * @return resultset of the query with header and first row as Hocon config object
    */
  def queryOne(hql: String, taskName: String) = {
    info(Util.prettyPrintAsciiBanner(hql,"query"))
    val effectiveHQL =
      s"""set mapred.job.name = $taskName;
         |set hive.cli.print.header=true;
         |$hql
       """.stripMargin
    val cmd = makeHiveCommand(effectiveHQL)
    val parser = new HQLReadParser
    executeCmd(cmd, stdout = parser)
    parser.getData
  }


  /**
    * 
    * @param hql
    * @param taskName
    * @return
    */
  def execute(hql: String, taskName: String) = {
    info(Util.prettyPrintAsciiBanner(hql,"query"))
    val effectiveHQL = s"set mapred.job.name = $taskName;\n" + hql
    val cmd = makeHiveCommand(effectiveHQL)
    val logParser = new HQLExecuteParser
    val retCode = executeCmd(cmd, stderr = logParser)
    assert(retCode == 0, s"query execution failed with ret code $retCode")
    Util.mapToConfig(logParser.rowsLoaded.toMap)
  }

  def makeHiveCommand(sql: String): Seq[String] = {
    val file = TaskContext.getTaskFile("query.hql")
    file <<= sql
    getExecutablePath("hive") match {
      case Some(exe) => exe :: "-f" :: file.toPath.toString :: Nil
      case None => throw new RuntimeException("hive command not found")
    }
  }

}
