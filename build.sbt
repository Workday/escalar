publishMavenStyle := true
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
pomIncludeRepository := { _ => false }
pomExtra := (
  <url>https://github.com/Workday/escalar</url>
    <licenses>
      <license>
        <name>MIT</name>
        <url>https://opensource.org/licenses/MIT</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:Workday/escalar.git</url>
      <connection>scm:git:git@github.com:Workday/escalar.git</connection>
    </scm>
    <developers>
      <developer>
        <id>skeletalbassman</id>
        <name>Steven Helferich</name>
      </developer>
      <developer>
        <id>bojdell</id>
        <name>Bodecker Dellamaria</name>
      </developer>
    </developers>)