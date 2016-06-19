import com.typesafe.sbt.SbtGit.GitKeys._
import sbt._
import sbtunidoc.Plugin.UnidocKeys._
import Modules._

assemblySettings


coverageEnabled.in(ThisBuild ,Test, test) := true

//addCommandAlias("full-test", "clean;test;coverageReport")


lazy val artemisia = (project in file(".")).enablePlugins(JavaAppPackaging)
  .settings(General.settings("artemisia"))
  .settings(libraryDependencies ++= Artemisia.dependencies,
          mainClass in Compile := Some("tech.artemisia.core.Main"),
    fullRunTask(TaskKey[Unit]("docgen"), Compile, "tech.artemisia.core.DocGenerator", "/Users/chlr/dev/T800/projects/artemisia"))
  .dependsOn(commons % "compile->compile;test->test", localhost, mysql, postgres)


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


lazy val all = (project in file("all")).aggregate(artemisia ,commons,localhost, mysql, postgres)
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
















