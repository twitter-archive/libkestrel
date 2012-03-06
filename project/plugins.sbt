
resolvers ++= Option(System.getenv("SBT_PROXY_REPO")) map { url =>
  Seq("proxy-repo" at url)
} getOrElse {
  Seq(
    "twitter.com" at "http://maven.twttr.com/",
    "scala-tools" at "http://scala-tools.org/repo-releases/",
    "freemarker" at "http://freemarker.sourceforge.net/maven2/"
  )
} ++ Seq("local" at ("file:" + System.getProperty("user.home") + "/.m2/repo/"))

addSbtPlugin("com.twitter" % "standard-project2" % "0.0.5")
