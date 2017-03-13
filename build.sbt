publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomIncludeRepository := { _ => false }
publishArtifact in Test := false

pgpSecretRing := file("local.secring.gpg")

pgpPublicRing := file("local.pubring.gpg")