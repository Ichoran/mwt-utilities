import mill._
import mill.scalalib._
import publish._

trait Common extends ScalaModule {
  def scalaVersion = "2.12.8"

  def scalacOptions = 
    T{ Seq("-unchecked", "-feature", "-deprecation", "-opt:l:method" )}

  def repositories() = super.repositories ++ Seq(
    coursier.maven.MavenRepository("https://oss.sonatype.org/content/repositories/snapshots")
  )

  def ivyDeps = Agg(
    ivy"com.twelvemonkeys.imageio:imageio-tiff:3.4.1",
    ivy"com.github.ichoran::kse:0.10-SNAPSHOT"
  )
}

object mwt_utilities extends Common with PublishModule {
  def publishVersion = "0.4.0-SNAPSHOT"
  def pomSettings = PomSettings(
    description = "Utilities to help Scala read data from MWT output",
    organization = "com.github.ichoran",
    url = "https://github.com/ichoran/mwt-utilities",
    licenses = Seq(License.`Apache-2.0`),
    versionControl = VersionControl.github("ichoran", "mwt-utilities"),
    developers = Seq(
      Developer("ichoran", "Rex Kerr", "https://github.com/ichoran")
    )
  )
}
