import sbt.Keys._
import sbt._

object Build extends Build {
  val EsClientVersion = "latest.integration"

  lazy val root = Project(id = "scala-elasticsearch-client-root", base = file("."))
    .aggregate(client)

  lazy val client = Project(id = "client", base = file("client"))
    .settings(
      name := "client",
      libraryDependencies ++= Seq(
        "io.searchbox" % "jest" % "0.1.7",
        "io.circe" %% "circe-core" % "0.7.0",
        "io.circe" %% "circe-generic" % "0.7.0",
        "io.circe" %% "circe-parser" % "0.7.0",
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.3.2",
        "org.mockito" % "mockito-core" % "1.9.5",
        "org.scalatest" %% "scalatest" % "2.1.2" % "test"
      ),
      externalResolvers += DefaultMavenRepository,
      resolvers += Resolver.sonatypeRepo("releases"),
      addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
    )

}