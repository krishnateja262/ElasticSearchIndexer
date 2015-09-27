name := "ElasticSearchIndexer"

version := "0.1-SNAPSHOT"

organization := "com.github.krishnateja262"

scalaVersion := "2.11.7"

enablePlugins(JavaAppPackaging)

packageDescription in Debian := "ElasticSearchIndexer"

maintainer in Debian := "Krishna"

libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-core" % "1.6.0"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.7.2"

libraryDependencies += "org.json" % "json" % "20140107"

libraryDependencies += "joda-time" % "joda-time" % "2.8.1"
