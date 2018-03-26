name := """play-scala-engine-neo4j"""
organization := "com.vector"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.12.4"

libraryDependencies += guice
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test

// https://mvnrepository.com/artifact/org.neo4j.driver/neo4j-java-driver
libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.5.1"

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "com.vector.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "com.vector.binders._"
