package tech.artemisia.dag

/**
 * Created by chlr on 1/7/16.
 */
object Status extends Enumeration {
  val READY,RUNNING,SUCCEEDED,FAILED,UNKNOWN,SKIPPED,FAILURE_IGNORED = Value
}

class DagException(message: String) extends Exception(message)

