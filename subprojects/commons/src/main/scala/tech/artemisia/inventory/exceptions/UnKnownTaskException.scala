package tech.artemisia.inventory.exceptions

/**
  * Created by chlr on 6/4/16.
  */

/**
  * This exception is thrown if an unknown task is requested.
  * @param message
  */
class UnknownTaskException(message: String) extends Exception(message)