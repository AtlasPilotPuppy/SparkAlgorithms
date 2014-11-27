name := "KnnJoin"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies  += "org.scalanlp" %% "breeze" % "0.8.1"

libraryDependencies  += "org.scalanlp" %% "breeze-natives" % "0.8.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.0.0-SNAPSHOT" % "provided"

