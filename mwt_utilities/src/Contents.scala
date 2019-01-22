package mwt.utilities

import java.io._
import java.nio.file._
import java.time._
import java.util.zip._

import scala.collection.JavaConverters._

import kse.flow._
import kse.eio._


abstract class BlobVisitor {
  def start(id: Int): Boolean = true
  def accept(line: String): Boolean
  def stop(): Unit = {}
}

case class Contents[A](who: Path, target: OutputTarget, baseString: String, base: A, summary: Option[String], blobs: Array[String], images: Array[String], others: Map[String, Array[String]]) {
  def summaryWalk[U](f: String => U): Ok[String, Unit] =
    if (summary.isEmpty) No(s"No summary file in $who")
    else safe {
      val filename = summary.get
      if (target.isZip) {
        val zf = new ZipFile(who.toFile)
        try {
          val ze = zf.entries.asScala.find(_.getName == filename) TossAs s"Cannot find $filename in $who"
          zf.getInputStream(ze).reader.lines.forEach(line => f(line))
        }
        finally { zf.close }
      }
      else Files.lines(who resolve filename).forEach(line => f(line))
    }.mapNo(e => s"Could not read ${summary.get} in $who:\n${e.explain(10)}\n")
  def summaryLines: Ok[String, Stored.Text] = {
    val vb = Vector.newBuilder[String]
    summaryWalk{ vb += _ }.map(_ => Stored.Text(vb.result()))
  }
  def theSummary: Ok[String, Summary] = summaryLines.flatMap(Summary from _.lines)

  private[utilities] def getIdFromBlobName(name: String, seen: collection.mutable.Set[Int]): Ok[String, Int] = {
    val i = name.lastIndexOf('.')
    val j = name.indexOf('_')
    if (i < 0) No(s"$name is not a blob file")
    else if (j < 0 || j >= i-2) No(s"$name does not end with _<number>.blob")
    else {
      val token = name.substring(j+1, i)
      Grok(token).oI match {
        case None    => No(s"$token in blob file $name is not a number")
        case Some(x) => 
          if (x <= 0)       No(s"Blob number $x in $name is out of range (must be a positive integer)")
          else if (seen(x)) No(s"Blob number $x in $name appeared earlier; numbers must be unique")
          else { seen += x; Yes(x) }
      }
    }
  }

  private[utilities] def getIdFromBlobsLine(line: String, file: String, n: Int, seen: collection.mutable.Set[Int]): Ok[String, Int] =
    if (line.isEmpty || line(0) != '%') No(s"Line $n in $file does not start with %: $line")
    else {
      val g = Grok(line)
      if (g.trySkip(1) < 1) No(s"Line $n in $file needs a blob ID after the %: $line")
      else g.oI match {
        case None    => No(s"Line $n in $file needs a number after the %: $line")
        case Some(x) => 
          if (x <= 0)       No(s"Line $n in $file has an out-of range blob number, $x (must be a positive integer)")
          else if (seen(x)) No(s"Line $n in $file has blob number $x which appeared earlier; numbers must be unique")
          else { seen += x; Yes(x) }
      }
    }

  def blobsVisit(visitor: BlobVisitor): Ok[String, Unit] = {
    val seen = collection.mutable.Set.empty[Int]
    safe {
      val seen = collection.mutable.Set.empty[Int]
      if (target.isZip) {
        val blobSet = blobs.toSet
        val zf = new ZipFile(who.toFile)
        try {
          val zei = zf.entries.asScala
          while (zei.hasNext) { 
            val ze = zei.next
            if (blobSet contains ze.getName) {
              if (ze.getName endsWith ".blob") {
                if (visitor.start(getIdFromBlobName(ze.getName, seen).?)) {
                  zf.getInputStream(ze).reader.lines.forEach(visitor accept _)
                  visitor.stop()
                }
              }
              else {
                var n = 0
                var id = 0
                var visit = false
                zf.getInputStream(ze).reader.lines.forEach{ line =>
                  n += 1
                  if (line.length > 0 && line.charAt(0) == '%') {
                    if (visit) visitor.stop()
                    id = getIdFromBlobsLine(line, ze.getName, n, seen).?
                    visit = visitor.start(id)
                  }
                  else if (line.length > 0 && visit) visitor accept line
                }
                if (visit) visitor.stop()
              }
            }
          }
        }
        finally { zf.close }
      }
      else {
        blobs.foreach{ blob =>
          if (blob endsWith ".blob") {
            if (visitor.start(getIdFromBlobName(blob, seen).?)) {
              Files.lines(who resolve blob).forEach(visitor accept _)
              visitor.stop()
            }
          }
          else {
            var n = 0
            var id = 0
            var visit = false
            Files.lines(who resolve blob).forEach{ line =>
              n += 1
              if (line.length > 0 && line.charAt(0) == '%') {
                if (visit) visitor.stop()
                id = getIdFromBlobsLine(line, blob, n, seen).?
                visit = visitor.start(id)
              }
              else if (line.length > 0 && visit) visitor accept line
            }
            if (visit) visitor.stop()
          }
        }
      }
    }.mapNo(e => s"Failed to process blobs from $who:\n${e.explain(16)}\n")
  }
  def blobLinesWalk[U](f: (Int, Stored.Text) => U) = blobsVisit(new BlobVisitor {
    private var myId: Int = -1
    private var myVb: collection.mutable.Builder[String, Vector[String]] = null
    override def start(id: Int) = { myId = id; myVb = Vector.newBuilder[String]; true }
    def accept(line: String) = { myVb += line; true }
    override def stop() { f(myId, Stored.Text(myVb.result())); myVb = null }
  })

  def imagesWalk(dispatch: String => ImageAcceptor): Ok[String, Unit] = 
    safe {
      if (target.isZip) {
        val imageSet = images.toSet
        val zf = new ZipFile(who.toFile)
        try {
          val zei = zf.entries.asScala
          while (zei.hasNext) { 
            val ze = zei.next
            if (imageSet contains ze.getName) {
              val acceptor = dispatch(ze.getName)
              if (acceptor.acceptable(ze.getName)) {
                acceptor.accept(zf.getInputStream(ze).gulp.?)
              }
            }
          }
        }
        finally { zf.close }
      }
      else {
        aFor(images){ (image, i) =>
          val acceptor = dispatch(image)
          if (acceptor.acceptable(image)) {
            acceptor.accept(Files.readAllBytes(who resolve image)).?
          }
        }
      }
    }.mapNo(e => s"Could not successfully process images from $who:\n${e.explain(12)}\n")
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

  def otherWalk(interpreter: String => Option[String => FromStore[Unit]]): Ok[String, Unit] = 
    safe {
      // Simplifies the inner logic if we can have these out here
      // and just not use them if it's not a zip file.
      // Using null instead of Option because why not?
      var zf: ZipFile = null
      var zes: Array[ZipEntry] = null
      try {
        for {
          (ext, files) <- others
          acceptor <- interpreter(ext)
        } {
          if (target.isZip) {
            val fileSet = files.toSet
            if (zf eq null)  zf = new ZipFile(who.toFile)
            if (zes eq null) zes = zf.entries.asScala.toArray
            zes.foreach{ ze =>
              if (fileSet contains ze.getName) acceptor(ze.getName) match {
                case fb: FromStore.Binary[Unit]  => fb(Stored.Binary(zf.getInputStream(ze).gulp.?))
                case ft: FromStore.Text[Unit]    => ft(Stored.Text(zf.getInputStream(ze).slurp.?))
              }
            }
          }
          else {
            aFor(files){ (file, i) => acceptor(file) match {
              case fb: FromStore.Binary[Unit]  => fb(Stored.Binary(Files.readAllBytes(who resolve file)))
              case ft: FromStore.Text[Unit] => 
                ft(Stored.Text({ 
                  val vb = Vector.newBuilder[String]
                  Files.lines(who resolve file).forEach(vb += _)
                  vb.result
                }))
            }
          }}
        }
      }
      finally { if (zf ne null) zf.close }
    }.mapNo(e => s"Failed to process other files from $who:\n${e.explain(24)}")
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
      if (j >= 0 || k >= 0) "" else s.substring(i)
    }
  }
  def from[A](p: Path, parser: String => Ok[String, A] = (s: String) => Ok.UnitYes, debug: Boolean = true): Ok[String, Contents[A]] = {
    val target = OutputTarget.from(p, true) TossAs (e => s"Not a MWT output target:\n$e")
    val listing = safe {
      val ab = Array.newBuilder[String]
      if (target.isZip) {
        val zf = new ZipFile(p.toFile)
        try {
          val zes = zf.entries
          zes.asScala.filterNot(_.isDirectory).foreach(ab += _.getName)
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
    val (blobs, notBlobOrSummary) = notSummaries.partition(f => f.endsWith(".blobs") || f.endsWith(".blob"))
    val sBase = summary.map(x => extractBase(clipOffDir(x))).toList
    val bBases = blobs.map(x => extractBase(clipOffDir(x), blobRules = true)).toList
    val base = ((sBase ::: bBases).toSet.toList: List[String]) match {
      case Nil => return No(s"No summary files or blob files in $p")
      case b :: Nil => b
      case clutter => return No(s"More than one data source in $p\nBase names found:\n  ${clutter.mkString("\n  ")}\n")
    }
    val parsedBase = parser(base) TossAs (e => s"Cannot interpret base filename pattern $base\n  $e")
    val (images, notBlobSummaryOrImage) =
      notBlobOrSummary.partition(f => ImageDecoder.library.contains(extractExt(f)))
    val otherMap = collection.mutable.AnyRefMap.empty[String, scala.collection.mutable.ArrayBuilder[String]]
    notBlobSummaryOrImage.foreach{ f =>
      val filename = clipOffDir(f)
      if (filename startsWith base) {
        val i = filename.lastIndexOf('.')
        val key = if (i < 0) filename else filename.substring(i+1)
        otherMap.getOrElseUpdate(key, Array.newBuilder[String]) += f
      }
    }
    Yes(Contents(p, target, base, parsedBase, summary, blobs.sorted, images.sorted, otherMap.mapValues(_.result().sorted).toMap))
  }

  object Implicits {
    implicit class FilesKnowAboutMWT(file: File) {
      def namedLikeMwtTaget: Boolean = {
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
