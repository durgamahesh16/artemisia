import sbt.Keys._
import sbt._

object Publish {

  val settings = Seq (
    publishMavenStyle  := true ,
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    pomExtra := <url>http://tech.artemisia/</url>
      <licenses>
        <license>
          <name>Apache License, Version 2.0</name>
          <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
        </license>
      </licenses>

      <scm>
        <url>git@github.com:mig-foxbat/artemisia.git</url>
        <connection>scm:git:git@github.com:mig-foxbat/artemisia.git</connection>
      </scm>
      <developers>
        <developer>
          <id>chlr</id>
          <name>Charles</name>
        </developer>
      </developers>
  )

}