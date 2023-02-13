ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "stunnel"
ThisBuild / scalaVersion := "3.2.2"

val CatsEffectVersion = "3.3.12"
val Http4sVersion = "0.23.6"
val CirceVersion = "0.14.3"

lazy val root = (project in file(".")).settings(
  name := "stuck-in-tunnel",
  libraryDependencies ++= Seq(
    "org.http4s"      %% "http4s-ember-server"  % Http4sVersion,
    "org.http4s"      %%  "http4s-ember-client" % Http4sVersion,
    "org.http4s"      %%  "http4s-circe"        % Http4sVersion,
    "org.http4s"      %%  "http4s-dsl"          % Http4sVersion,
    "io.circe"        %%  "circe-parser"        % CirceVersion,
    "io.circe"        %%  "circe-generic"       % CirceVersion,

    "ch.qos.logback"   %  "logback-classic"     % "1.2.6",

    // This pulls in the kernel and std modules automatically.
    "org.typelevel"   %% "cats-effect"          % CatsEffectVersion,
    "org.typelevel"   %% "cats-effect-kernel"   % CatsEffectVersion,
    "org.typelevel"   %% "cats-effect-std"      % CatsEffectVersion,
    "co.fs2"          %% "fs2-core"             % "3.6.1",

    "org.scalatest"   %% "scalatest"            % "3.2.15" % "test",
  )
)
