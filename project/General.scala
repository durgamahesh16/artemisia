import sbt.Keys._
import sbt._

object General {
  val group_id = "tech.artemisia"
  val mainScalaVersion = "2.11.7"
  val appVersion = "0.1-SNAPSHOT"
  val subprojectBase = file("subprojects")
  val componentBase = subprojectBase / "components"
  val dependencies = Seq (
    "ch.qos.logback" % "logback-classic" % "0.9.28",
    "ch.qos.logback" % "logback-classic" % "0.9.28" % "runtime",
    "org.slf4j" % "slf4j-api" % "1.7.6" % "provided",
    "org.slf4j" % "slf4j-nop" % "1.7.6" % "test",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    "org.pegdown" % "pegdown" % "1.0.2" % "test",
    "joda-time" % "joda-time" % "2.0",
    "com.typesafe" % "config" % "1.2.1"
  )
  val crossVersions =  Seq("2.10.6", mainScalaVersion)




  def settings(module: String, publishable: Boolean = true) = Seq(
    name := module,
    organization := General.group_id,
    version := General.appVersion,
    scalaVersion := General.mainScalaVersion,
    libraryDependencies ++= General.dependencies,
    crossScalaVersions := General.crossVersions,
    (dependencyClasspath in Test) <<= (dependencyClasspath in Test) map {
      _.filterNot(_.data.name.contains("logback-classic"))
    },
    resolvers += Resolver.jcenterRepo,

    credentials += Credentials(
      "Sonatype Nexus Repository Manager",
      "oss.sonatype.org",
      Option(System.getenv().get("SONATYPE_USERNAME")).getOrElse("NOT FOUND!!!"),
      Option(System.getenv().get("SONATYPE_PASSWORD")).getOrElse("NOT FOUND!!!")
    )
  ) ++ (if (publishable) Publish.settings else Seq())

}

