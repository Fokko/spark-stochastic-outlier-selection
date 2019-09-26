
name := "Spark Stochastic Outlier Selection"

version := "0.2.0"

publishTo := sonatypePublishToBundle.value

sonatypeProfileName := "frl.driesprong"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// Open-source license of your choice
licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("Fokko", "spark-stochastic-outlier-selection", "fokko@apache.org"))

// or if you want to set these fields manually
homepage := Some(url("https://github.com/Fokko/spark-stochastic-outlier-selection"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/Fokko/spark-stochastic-outlier-selection"),
    "scm:git@github.com:Fokko/spark-stochastic-outlier-selection.git"
  )
)
developers := List(
  Developer(id="Fokko", name="Fokko Driesprong", email="fokko@apache.org", url=url("https://github.com/Fokko"))
)

crossScalaVersions := Seq("2.11.12", "2.12.10")

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)
