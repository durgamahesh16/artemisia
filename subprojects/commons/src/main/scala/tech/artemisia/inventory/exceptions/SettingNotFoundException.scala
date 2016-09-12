package tech.artemisia.inventory.exceptions

/**
 * Created by chlr on 4/26/16.
 */


sealed abstract class SettingException(message: String) extends Throwable(message)

/**
 * Throw this exception when mandatory setting keys are missing
 * @param message message description
 */
class SettingNotFoundException(message: String) extends SettingException(message)


/**
  * This exception is thrown when an invalid value is provided for an setting
  * @param message message description
  */
class InvalidSettingException(message: String) extends SettingException(message)
