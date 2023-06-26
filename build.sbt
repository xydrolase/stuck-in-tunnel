ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "stunnel"
ThisBuild / scalaVersion := "3.3.0"

val CatsEffectVersion = "3.3.12"
val Http4sVersion = "0.23.6"
val CirceVersion = "0.14.3"

lazy val root = (project in file(".")).settings(
  name := "stuck-in-tunnel",
  libraryDependencies ++= Seq(
    // "org.http4s"            %% "http4s-ember-server" % Http4sVersion,
    "org.http4s"            %% "http4s-ember-client" % Http4sVersion,
    "org.http4s"            %% "http4s-circe"        % Http4sVersion,
    "org.http4s"            %% "http4s-dsl"          % Http4sVersion,
    "io.circe"              %% "circe-parser"        % CirceVersion,
    "io.circe"              %% "circe-generic"       % CirceVersion,

    "ch.qos.logback"         % "logback-classic"     % "1.2.6",

    "org.typelevel"         %% "cats-effect"         % CatsEffectVersion,
    "org.typelevel"         %% "cats-effect-kernel"  % CatsEffectVersion,
    "org.typelevel"         %% "cats-effect-std"     % CatsEffectVersion,
    "co.fs2"                %% "fs2-core"            % "3.6.1",

    "org.apache.arrow"       % "arrow-vector"        % "12.0.0",
    "org.apache.arrow"       % "arrow-memory-netty"  % "12.0.0",
    "org.apache.arrow"       % "arrow-dataset"       % "12.0.0",

    "com.softwaremill.magnolia1_3" %% "magnolia" % "1.3.0",

    "org.locationtech.jts"   % "jts-core"            % "1.19.0",
    "org.scalatest"         %% "scalatest"           % "3.2.15" % "test",

    // aws
    "io.laserdisc"          %% "fs2-aws-s3"          % "6.0.1"
  ),
  assembly / mainClass := Some("stunnel.Application"),
  ThisBuild / assemblyMergeStrategy := {
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case x if x.endsWith("module-info.class") => MergeStrategy.discard
    case "arrow-git.properties" => MergeStrategy.discard
    case x =>
      val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
      oldStrategy(x)
  }
)
