package tech.artemisia.task.hadoop.hive

import java.io.PrintWriter

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
    * hive executable path:
    * eg: /usr/local/bin/hive
    */
  protected val hive = getExecutablePath("hive") match {
    case Some(exe) => exe
    case None => throw new RuntimeException("hive executable not found. make sure it is set in the path variable.")
  }

  /**
    * execute select query that returns a single row and parse the single row as Hocon config object
    * @param hql SELECT query to be executed
    * @param taskName name to be set for the hive mapred job
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
    val parser = new HQLReadParser(new PrintWriter(System.out, true))
    executeCmd(cmd, stdout = parser)
    parser.close()
    parser.getData
  }


  /**
    * execute DML/DDL HQL queries
    * @param hql hql query to be executed
    * @param taskName name to be set for the hive mapred job
    * @return config with stats on rows loaded.
    */
  def execute(hql: String, taskName: String) = {
    info(Util.prettyPrintAsciiBanner(hql,"query"))
    val effectiveHQL = s"set mapred.job.name = $taskName;\n" + hql
    val cmd = makeHiveCommand(effectiveHQL)
    val logParser = new HQLExecuteParser(new PrintWriter(System.err, true))
    val retCode = executeCmd(cmd, stderr = logParser)
    logParser.close()
    assert(retCode == 0, s"query execution failed with ret code $retCode")
    Util.mapToConfig(logParser.rowsLoaded.toMap)
  }


  /**
    *
    * @param hql hql query to be executed.
    * @return command to execute the hive query
    */
  private[hive] def makeHiveCommand(hql: String) = {
    val file = TaskContext.getTaskFile("query.hql")
    file <<= hql
    hive :: "-f" :: file.toPath.toString :: Nil
  }

}
