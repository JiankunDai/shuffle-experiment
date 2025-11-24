name := "spark-shuffle-experiment"
version := "1.0.0"

scalaVersion := "2.10.5"

val sparkVersion = "1.6.3"

// 依赖配置
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.typesafe" % "config" % "1.3.0"
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// 解析器配置
resolvers ++= Seq(
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases/",
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "JBoss Repository" at "https://repository.jboss.org/nexus/content/repositories/releases/"
)

// 编译器选项
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps",
  "-language:implicitConversions"
)

// 并行执行设置
parallelExecution in Test := false

// JVM选项
javaOptions in run ++= Seq(
  "-Xms512M", 
  "-Xmx2G",
  "-XX:MaxPermSize=512M"
)

mainClass in Compile := Some("edu.ecnu.ShuffleExperiment")