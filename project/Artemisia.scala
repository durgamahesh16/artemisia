import sbt._

object  Artemisia {

  val dependencies = Seq (
    "com.github.scopt" %% "scopt" % "3.3.0",
    "com.typesafe.akka" %% "akka-actor" % "2.3.14",
    "org.scalaz" %% "scalaz-core" % "7.2.0",
    "com.typesafe.akka" %% "akka-testkit" % "2.4.1" % "test",
    "com.twitter" %% "util-eval" % "6.33.0",
    "org.scala-lang" % "scala-reflect" % General.mainScalaVersion,
    "org.scala-lang" % "scala-compiler" % General.mainScalaVersion,
    "org.yaml" % "snakeyaml" % "1.17"
  )



}