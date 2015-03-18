uniform.project("permafrost", "au.com.cba.omnia.permafrost")

uniformDependencySettings

libraryDependencies :=
  depend.hadoop() ++ depend.scalaz() ++ depend.testing() ++
  depend.omnia("omnitool-core", "1.8.0-20150318034256-6b79776") ++
  Seq(
    "org.apache.avro"   % "avro-mapred"   % "1.7.6",
    "au.com.cba.omnia" %% "omnitool-core" % "1.8.0-20150318034256-6b79776" % "test" classifier "tests"
  )

updateOptions := updateOptions.value.withCachedResolution(true)

publishArtifact in Test := true

uniform.docSettings("https://github.com/CommBank/permafrost")

uniform.ghsettings
