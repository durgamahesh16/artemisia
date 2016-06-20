import sbt._

object Commons {

  val dependencies = Seq (
     "com.google.guava" % "guava" % "19.0",
     "com.opencsv" % "opencsv" % "3.7",
     "com.h2database" % "h2" % "1.4.191" % "test",
     "commons-io" % "commons-io" % "2.5"
  )

}