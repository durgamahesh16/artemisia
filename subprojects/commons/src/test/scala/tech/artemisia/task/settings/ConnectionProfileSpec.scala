package tech.artemisia.task.settings

import com.typesafe.config.ConfigFactory
import tech.artemisia.TestSpec
import tech.artemisia.core.Keywords
import tech.artemisia.core.Keywords.Connection

/**
 * Created by chlr on 4/16/16.
 */
class ConnectionProfileSpec extends TestSpec {

  "ConnectionProfile" must "correctly construct the object from config" in {
    val config = ConfigFactory parseString
     s"""
        |	{
        |		${Connection.HOSTNAME} = "database-host"
        |		${Keywords.Connection.USERNAME} = "tango"
        |		${Keywords.Connection.PASSWORD} = "bravo"
        |		${Keywords.Connection.DATABASE} = "november"
        |		${Keywords.Connection.PORT} = 1000
        |	}
      """.stripMargin

    val connectionProfile = DBConnection(config)
    connectionProfile.hostname must be ("database-host")
    connectionProfile.username must be ("tango")
    connectionProfile.password must be ("bravo")
    connectionProfile.default_database must be ("november")
    connectionProfile.port must be (1000)
  }
}
