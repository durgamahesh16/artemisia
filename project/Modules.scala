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
    val settings = Seq(libraryDependencies ++= Seq("org.apache.commons" % "commons-email" % "1.2"))
  }

}