ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18" // Spark 3.5 is compatible with Scala 2.12

lazy val root = (project in file("."))
  .settings(
    name := "spark-nlp-labs",
    // Enable forking a new JVM for 'run' and 'test' tasks
    // This is necessary to pass JVM options for Spark on Java 9+
    fork := true,
    // Add JVM options to allow Spark to access internal Java APIs
    javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.nio.channels=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-mllib" % "3.5.1"
    )
  )
