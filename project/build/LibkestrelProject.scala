import sbt._
import Process._
import com.twitter.sbt._

/**
 * Sbt project files are written in a DSL in scala.
 *
 * The % operator is just turning strings into maven dependency declarations, so lines like
 *     val example = "com.example" % "exampleland" % "1.0.3"
 * mean to add a dependency on exampleland version 1.0.3 from provider "com.example".
 */
class LibkestrelProject(info: ProjectInfo) extends StandardServiceProject(info) 
  with NoisyDependencies
  with DefaultRepos
  with SubversionPublisher
  with PublishSourcesAndJavadocs
  with PublishSite
{
  val util_core = "com.twitter" % "util-core" % "1.12.3"
  val util_logging = "com.twitter" % "util-logging" % "1.12.3"

  // for tests
  val scalatest = "org.scalatest" % "scalatest_2.8.1" % "1.5.1" % "test"
  val scopt = "com.github.scopt" %% "scopt" % "1.1.1" % "test"

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
