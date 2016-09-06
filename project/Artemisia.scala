import sbt._

object  Artemisia {

  val dependencies = Seq (
    "com.github.scopt" %% "scopt" % "3.3.0",
    "com.typesafe.akka" %% "akka-actor" % "2.3.15",
    "org.scalaz" %% "scalaz-core" % "7.2.0",
    "com.typesafe.akka" %% "akka-testkit" % "2.3.15" % "test",
    "com.twitter" %% "util-eval" % "6.33.0",
    "org.yaml" % "snakeyaml" % "1.17"
  )

}