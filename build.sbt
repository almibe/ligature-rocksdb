import Dependencies._

ThisBuild / scalaVersion     := "2.13.2"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "dev.ligature"
ThisBuild / organizationName := "Ligature"

resolvers += Resolver.mavenLocal

lazy val root = (project in file("."))
  .settings(
    name := "ligature-rocksdb",
    libraryDependencies += "dev.ligature" %% "ligature" % "0.1.0-SNAPSHOT",
    libraryDependencies += "org.rocksdb" % "rocksdbjni" % "6.6.4",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "dev.ligature" %% "ligature-test-suite" % "0.1.0-SNAPSHOT" % Test
  )
