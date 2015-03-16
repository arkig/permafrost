uniform.project("permafrost", "au.com.cba.omnia.permafrost")

uniformDependencySettings

libraryDependencies :=
  depend.hadoop() ++ depend.scalaz() ++ depend.testing() ++
  depend.omnia("omnitool-core", "1.6.0-20150316042143-84a0449") ++
  Seq(
    "org.apache.avro"   % "avro-mapred"   % "1.7.4",
    "au.com.cba.omnia" %% "omnitool-core" % "1.6.0-20150316042143-84a0449" % "test" classifier "tests"
  )

updateOptions := updateOptions.value.withCachedResolution(true)

publishArtifact in Test := true

uniform.docSettings("https://github.com/CommBank/permafrost")

uniform.ghsettings
