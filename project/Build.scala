import sbt.Keys._
import sbt._

object Build extends Build {
  val EsClientVersion = "latest.integration"

  lazy val root = Project(id = "scala-elasticsearch-client-root", base = file("."))
    .settings(
      libraryDependencies ++= Seq(
        "io.circe" %% "circe-core" % "0.7.0",
        "io.circe" %% "circe-generic" % "0.7.0",
        "io.circe" %% "circe-parser" % "0.7.0",
        "io.circe" %% "circe-jackson28" % "0.7.0"
      ),
      resolvers += Resolver.sonatypeRepo("releases"),
      addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
    ) aggregate(client)

  lazy val client = Project(id = "client", base = file("client"))
    .settings(
      name := "client",
      libraryDependencies ++= Seq(
        "io.searchbox" % "jest" % "0.1.7",
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.3.2",
        "org.mockito" % "mockito-core" % "1.9.5",
        "org.scalatest" %% "scalatest" % "2.1.2" % "test",
        "io.circe" %% "circe-core" % "0.7.0",
        "io.circe" %% "circe-generic" % "0.7.0",
        "io.circe" %% "circe-parser" % "0.7.0",
        "io.circe" %% "circe-jackson28" % "0.7.0"
      ),
      externalResolvers += DefaultMavenRepository,
      resolvers += Resolver.sonatypeRepo("releases"),
      addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
    )
}