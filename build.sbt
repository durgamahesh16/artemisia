import Modules._
import com.typesafe.sbt.SbtGit.GitKeys._
import sbt.Attributed.data
import sbt.Keys._
import sbt._
import sbtunidoc.Plugin.UnidocKeys._

assemblySettings

coverageEnabled.in(ThisBuild ,Test, test) := true

parallelExecution in Global := false // this is required so that different modules that launch
  // that launch miniDFSCluster doesn't fail due to resource contention.

lazy val docgen = taskKey[Unit]("Generate Components documentation")

lazy val refgen = taskKey[Unit]("Generate settings conf file")

fork := true // This is required so that setting.file system property is properly set by javaOptions

javaOptions in Global += s"-Dsetting.file="+baseDirectory.value / "src/universal/conf/settings.conf"

stage in all <<= (stage in all) dependsOn refgen

stage in all <<= (stage in all) dependsOn docgen

javaOptions in Test ++= Seq(
    s"-Dsetting.file="+baseDirectory.value / "subprojects/commons/src/test/resources/settings.conf"
    ,"-Xmx4G"
)

resolvers in ThisBuild ++= Seq(
    "Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/"
    ,"cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

docgen := {
    val r = (runner in Compile).value
    val input = baseDirectory.value
    val cp = (fullClasspath in Compile).value
    toError(r.run("tech.artemisia.core.DocGenerator", data(cp), Seq(input.toString), streams.value.log))
}

refgen := {
    val r = (runner in Compile).value
    val input = baseDirectory.value / "src/universal/conf/settings.conf"
    val cp = (fullClasspath in Compile).value
    toError(r.run("tech.artemisia.core.ReferenceGenerator", data(cp), Seq(input.toString), streams.value.log))
}

lazy val artemisia = (project in file(".")).enablePlugins(JavaAppPackaging)
  .settings(General.settings("artemisia"),
    bashScriptExtraDefines += """addJava "-Dsetting.file=${app_home}/../conf/settings.conf"""")
  .settings(libraryDependencies ++= Artemisia.dependencies,
            libraryDependencies ++= Seq(
                "org.scala-lang" % "scala-reflect" % scalaVersion.value,
                "org.scala-lang" % "scala-compiler" % scalaVersion.value
            ),
          mainClass in Compile := Some("tech.artemisia.core.Main"))
  .dependsOn(commons % "compile->compile;test->test", localhost, mysql, postgres, teradata, hive)


lazy val localhost = (project in General.componentBase / "localhost").enablePlugins(JavaAppPackaging)
  .settings(General.settings("localhost")).dependsOn(commons  % "compile->compile;test->test")
  .settings(Core.settings)


lazy val commons = (project in General.subprojectBase / "commons").enablePlugins(JavaAppPackaging)
  .settings(General.settings("commons"))
  .settings(libraryDependencies ++= Commons.dependencies)


lazy val mysql = (project in General.componentBase / "database" / "mysql").enablePlugins(JavaAppPackaging)
  .settings(General.settings("mysql"))
  .dependsOn(commons  % "compile->compile;test->test")
  .settings(MySQL.settings)


lazy val postgres = (project in General.componentBase / "database" / "postgres").enablePlugins(JavaAppPackaging)
  .settings(General.settings("postgres"))
  .dependsOn(commons  % "compile->compile;test->test")
  .settings(Postgres.settings)


lazy val teradata = (project in General.componentBase / "database" / "teradata").enablePlugins(JavaAppPackaging)
  .settings(General.settings("teradata"))
  .dependsOn(commons  % "compile->compile;test->test")
  .dependsOn(hive)

lazy val hive = (project in General.componentBase / "hadoop" / "hive").enablePlugins(JavaAppPackaging)
  .settings(General.settings("hive"))
  .settings(Hive.settings)
  .dependsOn(commons  % "compile->compile;test->test")


lazy val all = (project in file("all")).aggregate(artemisia ,commons,localhost, mysql, postgres, teradata, hive)
  .enablePlugins(JavaAppPackaging)
  .settings(General.settings("all", publishable = false))
  .settings(
    publishArtifact := false
    ,publish := {}
    ,publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))))
  .settings(unidocSettings)
  .settings(site.settings ++ ghpages.settings: _*)
  .settings(
    coverageEnabled := true,
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject -- inProjects(artemisia),
    site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "latest/api"),
    gitRemoteRepo := "git@github.com:mig-foxbat/artemesia.git"
  )
















