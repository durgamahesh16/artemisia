import sbt._

object Commons {

  val dependencies = Seq (
     "com.google.guava" % "guava" % "19.0",
     "com.opencsv" % "opencsv" % "3.7",
     "com.h2database" % "h2" % "1.4.191" % "test",
     "org.apache.commons" % "commons-exec" % "1.3",
     "commons-io" % "commons-io" % "2.5",
     "org.ostermiller" % "utils" % "1.07.00",
     "org.apache.hadoop" % "hadoop-client" % "2.7.2" % "provided",
     "org.apache.hadoop" % "hadoop-hdfs" % "2.7.2" % "test",
     "org.apache.hadoop" % "hadoop-common" % "2.7.2" % "test",
     "org.apache.hadoop" % "hadoop-hdfs" % "2.7.2" % "test" classifier "tests",
     "org.apache.hadoop" % "hadoop-common" % "2.7.2" % "test" classifier "tests"
   )

}