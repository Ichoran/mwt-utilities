package mwt.utilities

import java.io._
import java.nio.file._
import java.time._

import scala.collection.JavaConverters._

import kse.flow._
import kse.eio._


abstract class BlobVisitor {
  def start(id: Int): Boolean = true
  def accept(line: String): Boolean
  def stop(): Unit = {}
}

case class Contents[A](who: Path, target: OutputTarget, base: A, summary: Option[String], blobs: Array[String], images: Array[String], others: Map[String, Array[String]]) {
  def summaryWalk[U](f: String => U): Ok[String, Unit] = {
    val filename = summary.toOk.mapNo(_ => s"No summary file in $who").?
    if (target.isZip) safe {
      val zf = new java.util.zip.ZipFile(who.toFile)
      try {
        val ze = zf.entries.asScala.find(_.getName == filename).toOk.mapNo(_ => s"Cannot find $filename in $who").?
        val reader = new BufferedReader(new InputStreamReader(zf.getInputStream(ze)))
        reader.lines.forEach(line => f(line))
      }
      finally { zf.close }
    }.mapNo(e => s"Could not extract $filename from $who:\n${e.explain(12)}\n")
    else safe {
      Files.lines(who resolve filename).forEach(line => f(line))
    }.mapNo(e => s"Could not read $filename in $who:\n${e.explain(10)}\n")
  }
  def summaryLines: Ok[String, Vector[String]] = {
    val vb = Vector.newBuilder[String]
    summaryWalk{ vb += _ }.map(_ => vb.result())
  }

  def blobsVisit(visitor: BlobVisitor): Ok[String, Unit] = ???
  def blobLinesWalk[U](f: (Int, Vector[String]) => U) = blobsVisit(new BlobVisitor {
    private var myId: Int = -1
    private var myVb: collection.mutable.Builder[String, Vector[String]] = null
    override def start(id: Int) = { myId = id; myVb = Vector.newBuilder[String]; true }
    def accept(line: String) = { myVb += line; true }
    override def stop() { f(myId, myVb.result()); myVb = null }
  })

  def imagesWalk(dispatch: String => ImageAcceptor): Ok[String, Unit] = 
    if (target.isZip) safe {
      ???
    }.mapNo(e => s"Could not successfully process images from $who:\n${e.explain(12)}\n")
    else safe {
      aFor(images){ (image, i) =>
        val acceptor = dispatch(image)
        if (acceptor.acceptable(image)) {
          acceptor.accept(Files.readAllBytes(who resolve image)).?
        }
      }
    }.mapNo(e => s"Could not successfully process images in $who:\n${e.explain(10)}\n")
  def imagesDecoded(p: String => Boolean): Ok[String, Array[(String, java.awt.image.BufferedImage)]] = {
    val ans = Array.newBuilder[(String, java.awt.image.BufferedImage)]
    imagesWalk{ filename =>
      if (!p(filename)) ImageDecoder.DoNotDecode
      val i = filename.lastIndexOf('.')
      val key = if (i < 0) "" else filename.substring(i)
      ImageDecoder.library.get(key).flatten match {
        case Some(d) => d.asAcceptor(img => ans += (filename -> img))
        case None    => ImageDecoder.DoNotDecode
      }
    }.map(_ => ans.result())
  }

  def otherWalk(interpreter: String => Option[Either[Array[Byte] => Unit, Vector[String] => Unit]]) = ???
}
object Contents {
  def clipOffDir(s: String): String = {
    val i = s.lastIndexOf('/')
    val j = s.lastIndexOf('\\')
    if (i < 0 && j < 0) s else s.substring((i max j)+1)
  }
  def extractBase(s: String, blobRules: Boolean = false): String = 
    if (blobRules) {
      val i = s.lastIndexOf('.')
      val j = s.lastIndexOf('_', if (i < 0) s.length else i)
      if (j < 0) s else s.substring(0, j)
    }
    else {
      val i = s.lastIndexOf('.')
      if (i < 0) s else s.substring(0, i)
    }
  def extractExt(s: String): String = {
    val i = s.lastIndexOf('.')
    if (i < 0) ""
    else {
      val j = s.indexOf('/', i)
      val k = s.indexOf('\\', i)
      if (j < 0 || k < 0) "" else s.substring(i)
    }
  }
  def from[A](p: Path, parser: String => Option[A], debug: Boolean = true): Ok[String, Contents[A]] = {
    val target = OutputTarget.from(p, true).mapNo(e => s"Not a MWT output target:\n$e").?
    val listing = safe {
      val ab = Array.newBuilder[String]
      if (target.isZip) {
        val zf = new java.util.zip.ZipFile(p.toFile)
        try {
          zf.entries.asScala.
            filterNot(_.isDirectory).
            foreach(ab += _.getName)
        }
        finally { zf.close }
      }
      else {
        if (!Files.isDirectory(p)) return No(s"Looks like but is not actually a directory: $p")
        Files.list(p).forEach(pi => ab += pi.getFileName.toString)
      }
      ab.result
    }.mapNo(e => s"Could not read $p\n$e\n").?
    val (summaries, notSummaries) = listing.partition(_ endsWith ".summary")
    val summary = summaries.toList match {
      case Nil => None
      case one :: Nil => Some(one)
      case one :: uhoh => return No(s"Too many summary files: $one ${uhoh.mkString(" ")}")
    }
    val (blobs, notBlobOrSummary) = notSummaries.partition(f => f.endsWith("blobs") || f.endsWith("blob"))
    val sBase = summary.map(x => extractBase(clipOffDir(x))).toList
    val bBases = blobs.map(x => extractBase(clipOffDir(x), blobRules = true)).toList
    val base = ((sBase ::: bBases).toSet.toList: List[String]) match {
      case Nil => return No(s"No summary files or blob files in $p")
      case b :: Nil => b
      case clutter => return No(s"More than one data source in $p\nBase names found:\n  ${clutter.mkString("\n  ")}\n")
    }
    val parsedBase = parser(base).toOk.mapNo(_ => s"Cannot interpret base filename pattern $base").?
    val (images, notBlobSummaryOrImage) =
      notBlobOrSummary.partition(f => ImageDecoder.library.contains(extractExt(f)))
    val otherMap = collection.mutable.AnyRefMap.empty[String, scala.collection.mutable.ArrayBuilder[String]]
    notBlobSummaryOrImage.foreach{ f =>
      val filename = clipOffDir(f)
      if (filename startsWith base) {
        val i = filename.lastIndexOf('.')
        val key = if (i < 0) filename else filename.substring(i+1)
        otherMap.getOrElseUpdate(key, Array.newBuilder[String]) += filename
      }
    }
    Yes(Contents(p, target, parsedBase, summary, blobs.sorted, images.sorted, otherMap.mapValues(_.result().sorted).toMap))
  }

  object Implicits {
    implicit class FilesKnowAboutMWT(file: File) {
      def namedLikeMwtDir: Boolean = {
        val parts = file.base.split('_')
        parts.length >= 2 &&
        parts(parts.length-2).fn{ s =>
          s.length == 8 &&
          s.forall(c => '0' <= c && c <= '9')
        } &&
        parts(parts.length-1).fn{ s =>
          s.take(6).forall(c => '0' <= c && c <= '9') &&
          (s.length == 6 || (s.length == 7 && s.last.toUpper == 'Z'))
        }
      }
    }
  }
}
