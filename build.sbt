uniform.project("permafrost", "au.com.cba.omnia.permafrost")

uniformDependencySettings

strictDependencySettings

val omnitoolVersion = "1.8.1-20150326034344-bbff728"

libraryDependencies :=
  depend.hadoopClasspath ++ depend.hadoop() ++ depend.testing() ++
  depend.omnia("omnitool-core", omnitoolVersion) ++
  Seq(
    noHadoop("org.apache.avro"  % "avro-mapred"   % depend.versions.avro),
    "au.com.cba.omnia"         %% "omnitool-core" % omnitoolVersion % "test" classifier "tests"
  )

updateOptions := updateOptions.value.withCachedResolution(true)

publishArtifact in Test := true

uniform.docSettings("https://github.com/CommBank/permafrost")

uniform.ghsettings
