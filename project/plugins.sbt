resolvers += Resolver.url("commbank-releases-ivy", new URL("http://commbank.artifactoryonline.com/commbank/ext-releases-local-ivy"))(Patterns("[organization]/[module]_[scalaVersion]_[sbtVersion]/[revision]/[artifact](-[classifier])-[revision].[ext]"))

val uniformVersion = "0.7.0-20150113035718-6641c80"

addSbtPlugin("au.com.cba.omnia" % "uniform-core"       % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-dependency" % uniformVersion)
