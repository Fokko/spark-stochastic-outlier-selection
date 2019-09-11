
name := "Spark Stochastic Outlier Selection"

version := "0.1.0"

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

lazy val core = (project in file(".")).settings(
  organization := "frl.driesprong"
  //other properties here
)

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/Fokko/spark-stochastic-outlier-selection</url>
    <licenses>
      <license>
        <name>The Apache License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:git@github.com:Fokko/spark-stochastic-outlier-selection.git</connection>
      <developerConnection>scm:git:git@github.com:Fokko/spark-stochastic-outlier-selection.git</developerConnection>
      <url>git@github.com:Fokko/spark-stochastic-outlier-selection.git</url>
    </scm>
    <developers>
      <developer>
        <name>Fokko Driesprong</name>
        <email>fokko@driesprong.frl</email>
        <organization>Driesprong</organization>
        <organizationUrl>http://driesprong.frl</organizationUrl>
      </developer>
    </developers>)

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)
