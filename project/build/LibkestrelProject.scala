import sbt._
import Process._
import com.twitter.sbt._

class LibkestrelProject(info: ProjectInfo) extends StandardServiceProject(info)
  with NoisyDependencies
  with DefaultRepos
  with SubversionPublisher
  with PublishSourcesAndJavadocs
  with PublishSite
{
  val util_core = buildScalaVersion match {
    case "2.8.1" => "com.twitter" % "util-core" % "1.12.13"
    case "2.9.1" => "com.twitter" % "util-core_2.9.1" % "1.12.13"
  }
  val util_logging = buildScalaVersion match {
    case "2.8.1" => "com.twitter" % "util-logging" % "1.12.13"
    case "2.9.1" => "com.twitter" % "util-logging_2.9.1" % "1.12.13"
  }

  // for tests
  val scalatest = "org.scalatest" %% "scalatest" % "1.7.1" % "test"
  val scopt = "com.github.scopt" %% "scopt" % "1.1.3" % "test"

  // use "<package>_<scalaversion>" as the published name:
  override def disableCrossPaths = false

  override def subversionRepository = Some("https://svn.twitter.biz/maven")

  // generate a jar that can be run for load tests.
  def loadTestJarFilename = "libkestrel-tests-" + version.toString + ".jar"
  def loadTestPaths = ((testCompilePath ##) ***) +++ ((mainCompilePath ##) ***)
  def packageLoadTestsAction =
    packageTask(loadTestPaths, outputPath, loadTestJarFilename, packageOptions) && task {
      distPath.asFile.mkdirs()
      FileUtilities.copyFlat(List(outputPath / loadTestJarFilename), distPath, log).left.toOption
    }
  lazy val packageLoadTests = packageLoadTestsAction
  override def packageDistTask = packageLoadTestsAction && super.packageDistTask
}
