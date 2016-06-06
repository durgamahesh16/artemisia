package tech.artemisia.inventory.exceptions

/**
 * Created by chlr on 4/26/16.
 */

/**
 * Throw this exception when mandatory setting keys are missing
 * @param message message describing the exception
 */
class SettingNotFoundException(message: String) extends Exception(message)
