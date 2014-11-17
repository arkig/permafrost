uniform.project("permafrost", "au.com.cba.omnia.permafrost")

uniformDependencySettings

libraryDependencies :=
  depend.hadoop() ++ depend.scalaz() ++ depend.testing() ++
  depend.omnia("omnitool-core", "1.7.0-20150316053109-4b4b011") ++
  Seq(
    "org.apache.avro"   % "avro-mapred"   % "1.7.6",
    "au.com.cba.omnia" %% "omnitool-core" % "1.7.0-20150316053109-4b4b011" % "test" classifier "tests"
  )

updateOptions := updateOptions.value.withCachedResolution(true)

publishArtifact in Test := true

uniform.docSettings("https://github.com/CommBank/permafrost")

uniform.ghsettings
