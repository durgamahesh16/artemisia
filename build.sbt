import com.typesafe.sbt.SbtGit.GitKeys._
import sbt._
import sbtunidoc.Plugin.UnidocKeys._
import Modules._

assemblySettings


coverageEnabled.in(ThisBuild ,Test, test) := true

//addCommandAlias("full-test", "clean;test;coverageReport")


lazy val artemisia = (project in file(".")).enablePlugins(JavaAppPackaging)
  .settings(General.settings("artemisia"))
  .settings(libraryDependencies ++= Artemisia.dependencies)
  .dependsOn(commons % "compile->compile;test->test", localhost, mysql)


lazy val localhost = (project in General.componentBase / "localhost").enablePlugins(JavaAppPackaging)
  .settings(General.settings("localhost")).dependsOn(commons  % "compile->compile;test->test")

lazy val commons = (project in General.subprojectBase / "commons").enablePlugins(JavaAppPackaging)
  .settings(General.settings("commons"))
  .settings(libraryDependencies ++= Commons.dependencies)


lazy val mysql = (project in General.componentBase / "database" / "mysql").enablePlugins(JavaAppPackaging)
  .settings(General.settings("mysql"))
  .dependsOn(commons  % "compile->compile;test->test")
  .settings(MySQL.settings)


lazy val all = (project in file("all")).aggregate(artemisia ,commons,localhost, mysql)
  .enablePlugins(JavaAppPackaging)
  .settings(General.settings("all"))
  .settings(unidocSettings)
  .settings(site.settings ++ ghpages.settings: _*)
  .settings(
    coverageEnabled := true,
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject -- inProjects(artemisia),
    site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "latest/api"),
    gitRemoteRepo := "git@github.com:mig-foxbat/artemesia.git"
  )
















