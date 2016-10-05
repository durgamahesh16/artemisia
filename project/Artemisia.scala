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


  val providedDependencies = Map {
    "hive" -> Seq {
      "org.apache.hive" % "hive-jdbc" % "0.10.0-cdh4.2.0"
    }
    "commons" -> Seq {
      "org.apache.hadoop" % "hadoop-client" % "2.7.2"
    }
  }


}