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
  val util_core = "com.twitter" % "util-core_2.9.1" % "1.12.13"
  val util_logging = "com.twitter" % "util-logging_2.9.1" % "1.12.13"

  // for tests
  val scalatest = "org.scalatest" % "scalatest_2.9.1" % "1.7.1" % "test"
  val scopt = "com.github.scopt" %% "scopt" % "1.1.3" % "test"

  override def subversionRepository = Some("http://svn.local.twitter.com/maven")

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
