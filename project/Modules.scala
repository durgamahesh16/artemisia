import sbt._
import sbt.Keys._


object Modules {

  object MySQL {
    val settings = Seq(libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.6")
  }

  object Postgres {
    val settings = Seq(libraryDependencies +=  "postgresql" % "postgresql" % "9.1-901-1.jdbc4")
  }

  object Core {
    val settings = Seq(libraryDependencies ++= Seq(
      "org.apache.commons" % "commons-email" % "1.2",
      "com.jcraft" % "jsch" % "0.1.53",
      "org.apache.sshd" % "sshd-core" % "1.2.0")
    )
  }

  object Hive {
    val settings = Seq(
      libraryDependencies += "org.apache.hive" % "hive-jdbc" % "0.10.0-cdh4.2.0" % "provided"
    )
  }

}