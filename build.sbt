ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "dagger",
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % """2.3.10"""
    )
  )
