package mwt.utilities

import java.io._
import java.nio.file._
import java.time._

import scala.collection.JavaConverters._

import kse.flow._
import kse.eio._

final case class OutputTarget(name: String, when: Instant, prefix: Option[String]) {
  def isZip = name endsWith ".zip"
}
object OutputTarget {
  def from(s: String, debug: Boolean = true): Ok[String, OutputTarget] = s.split('_').pipe{ parts =>
    def no(why: => String) = No(if (debug) why else "Misformatted")

    if (parts.length < 2) return no(s"Too few parts (${parts.length})")
    val date = parts(parts.length-2)
    val time = parts(parts.length-1).take(6)
    if (date.length != 8) return no(s"Date chunk is not 8 chars long: $date")
    if (time.length < 6) return no(s"Time chunk is less than 6 chars: $time")
    nFor(date.length){ i => val c = date.charAt(i); if (c < '0' || c > '9') return no("Date char #${i+1} is not a digit: $c") }
    nFor(time.length){ i => val c = time.charAt(i); if (c < '0' || c > '9') return no("Time char #${i+1} is not a digit: $c") }
    val zoned = parts(parts.length-1).pipe{ bit =>
      if (bit.length == 6) false
      else if (bit.length == 7) {
        if (bit.charAt(6) == 'Z') true else return no("Last part right length to end with Z but ends with ${bit.charAt(6)}")
      }
      else if (bit.length == 10) {
        if (bit endsWith ".zip") false else return no("Last part right length to end with .zip but ends with ${bit.drop(6)}")
      }
      else if (bit.length == 11) {
        if (bit endsWith "Z.zip") true else return no("Last part right length to end with Z.zip but ends with ${bit.drop(6)}")
      }
      else return No("Wrong size for last part (${bit.length})")
    }
    val instant = safe {
      val y = date.substring(0, 4).toInt
      val mo = date.substring(4, 6).toInt
      val d = date.substring(6, 8).toInt
      val h = time.substring(0, 2).toInt
      val min = time.substring(2, 4).toInt
      val sec = time.substring(4, 6).toInt
      Instant from ZonedDateTime.of(y, mo, d, h, min, sec, 0, if (zoned) ZoneOffset.UTC else ZoneId.systemDefault)
    }.mapNo(e => s"Error parsing date:\n${e.explain()}\n").?
    val prefix = (
      if (parts.length > 2) Some(s.dropRight(2 + date.length + parts(parts.length-1).length))
      else None
    )
    Yes(new OutputTarget(s, instant, prefix))
  }

  def from(f: File): Ok[String, OutputTarget] = from(f.getName)
  def from(f: File, debug: Boolean): Ok[String, OutputTarget] = from(f.getName, debug)

  def from(p: Path): Ok[String, OutputTarget] = from(p.getFileName.toString)
  def from(p: Path, debug: Boolean): Ok[String, OutputTarget] = from(p.getFileName.toString, debug)
}


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

  def imagesWalk(decoder: String => Option[Array[Byte] => Unit]): Ok[String, Unit] = ???
  def imagesDecoded(p: String => Boolean): Ok[String, Array[(String, java.awt.image.BufferedImage)]] = {
    val ans = Array.newBuilder[(String, java.awt.image.BufferedImage)]
    imagesWalk{ filename =>
      val inLibrary: Option[Array[Byte] => Ok[String, java.awt.image.BufferedImage]] =
        Contents.ImageReaders.library.get(filename).flatten.filter(_ => p(filename))
      inLibrary.
        map{ decoder: (Array[Byte] => Ok[String, java.awt.image.BufferedImage]) => 
          decoder andThen (result => {
            ans += ((filename, result.?))
            ()
          })
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
      notBlobOrSummary.partition(f => ImageReaders.library.exists{ case (k, _) => f endsWith k})
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

  object ImageReaders {
    import java.awt._
    import java.awt.image._
    import java.awt.color._

    val library: Map[String, Option[Array[Byte] => Ok[String, java.awt.image.BufferedImage]]] = Map(
      ".raw" -> Some(mwtFlavorRawReader _),
      ".raw8" -> None,
      ".tiff" -> None,
      ".tif" -> None,
      ".png" -> None,
      ".dbde" -> None
    )

    def mwtFlavorRawReader(bytes: Array[Byte]): Ok[String, java.awt.image.BufferedImage] = {
      val buf = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).asShortBuffer
      if (buf.remaining < 2) return No(s"Input data too small to contain image size (${bytes.length} bytes)")
      val nx = buf.get()
      val ny = buf.get()
      if (nx < 0 || ny < 0 || nx.toLong*ny.toLong >= Int.MaxValue) return No(s"Input data not a sensible size: $nx x $ny")
      if (buf.remaining < nx*ny) return No(s"Insufficient elements for image ($nx x $ny claimed but have only ${buf.remaining})")
      val data = new Array[Short](nx*ny)
      buf.get(data)
      val dbs = new DataBufferShort(data, data.length)
      val ccm = new ComponentColorModel(ColorSpace getInstance ColorSpace.CS_GRAY, false, true, Transparency.OPAQUE, DataBuffer.TYPE_SHORT)
      val wr = Raster.createWritableRaster(ccm.createCompatibleSampleModel(nx, ny), dbs, null)
      Yes(new BufferedImage(ccm, wr, false, null))
    }

    def mwtFlavorRaw8Reader(bytes: Array[Byte]): Ok[String, java.awt.image.BufferedImage] = {
      val buf = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN)
      if (buf.remaining < 4) return No(s"Input data too small to contain image size (${bytes.length} bytes)")
      val nx = buf.getShort()
      val ny = buf.getShort()
      if (nx < 0 || ny < 0 || nx.toLong*ny.toLong >= Int.MaxValue) return No(s"Input data not a sensible size: $nx x $ny")
      if (buf.remaining < nx*ny) return No(s"Insufficient elements for image ($nx x $ny claimed but have only ${buf.remaining})")
      val dbs =
        if (buf.remaining*0.8 > nx*ny) {
          val data = new Array[Byte](nx*ny)
          buf.get(data)
          new DataBufferByte(data, data.length)
        }
        else new DataBufferByte(bytes, 4, nx*ny)
      val ccm = new ComponentColorModel(ColorSpace getInstance ColorSpace.CS_GRAY, false, true, Transparency.OPAQUE, DataBuffer.TYPE_BYTE)
      val wr = Raster.createWritableRaster(ccm.createCompatibleSampleModel(nx, ny), dbs, null)
      Yes(new BufferedImage(ccm, wr, false, null))
    }
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
