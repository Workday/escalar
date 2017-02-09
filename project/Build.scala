import sbt.Keys._
import sbt._

object Build extends Build {
  val EsClientVersion = "latest.integration"

  lazy val root = Project(id = "scala-elasticsearch-client-root", base = file("."))
    .settings(
      scalaVersion := "2.10.4",
      libraryDependencies ++= Seq(
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.5.3",
        "com.fasterxml.jackson.core" % "jackson-annotations" % "2.5.3",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.3",
        "com.fasterxml.jackson.core" % "jackson-core" % "2.5.3"
      )
    ) aggregate(client)

  lazy val client = Project(id = "client", base = file("client"))
    .settings(
      name := "client",
      libraryDependencies ++= Seq(
        "io.searchbox" % "jest" % "0.1.7",
        "org.mockito" % "mockito-core" % "1.9.5",
        "org.scalatest" %% "scalatest" % "2.1.2" % "test",
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.5.3",
        "com.fasterxml.jackson.core" % "jackson-annotations" % "2.5.3",
        "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.3",
        "com.fasterxml.jackson.core" % "jackson-core" % "2.5.3"
      )//,
      //externalResolvers += DefaultMavenRepository
    )
}