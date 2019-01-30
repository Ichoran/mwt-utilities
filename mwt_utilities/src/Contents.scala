package mwt.utilities

import java.io._
import java.nio.file._
import java.nio.file.attribute.FileTime
import java.time._
import java.util.zip._

import scala.collection.JavaConverters._

import kse.flow._
import kse.eio._
import kse.maths._


abstract class BlobVisitor {
  def start(id: Int): Boolean = true
  def accept(line: String): Boolean
  def stop(): Unit = {}
}

abstract class FilesVisitor {
  def start(): Unit = {}
  def requestBlobs: Boolean = false
  def visitBlobData(name: String, modified: FileTime): FromStore[Unit] = { _ => () }: FromStore.Binary[Unit]
  def noMoreBlobs(): Unit = {}
  def requestSummary: Boolean = false
  def visitSummary(name: String, modified: FileTime): FromStore[Unit] = { _ => () }: FromStore.Binary[Unit]
  def noSummary(): Unit = {}
  def requestImages: Boolean = false
  def visitImage(name: String, modified: FileTime): FromStore[Unit] = { _ => () }: FromStore.Binary[Unit]
  def noMoreImages(): Unit = {}
  def requestOthers(category: String): Boolean = false
  def visitOther(category: String, name: String, modified: FileTime): FromStore[Unit] = { _ => () }: FromStore.Binary[Unit]
  def noMoreOfTheseOthers(): Unit = {}
  def stop(): Unit = {}
}
object FilesVisitor {
  def justSummary(callback: Array[Byte] => Unit): FilesVisitor = new FilesVisitor {
    override def requestSummary = true
    override def visitSummary(name: String, modified: FileTime): FromStore[Unit] =
      { x => callback(x.data) }: FromStore.Binary[Unit]
  }
  def justBlobData(callback: (String, Array[Byte]) => Unit): FilesVisitor = new FilesVisitor {
    override def requestBlobs = true
    override def visitBlobData(name: String, modified: FileTime): FromStore[Unit] = 
      { x => callback(name, x.data) }: FromStore.Binary[Unit]
  }
  def justImages(callback: (String, Array[Byte]) => Unit): FilesVisitor = new FilesVisitor {
    override def requestImages = true
    override def visitImage(name: String, modified: FileTime): FromStore[Unit] =
      { x => callback(name, x.data) }: FromStore.Binary[Unit]
  }
  def justOthers(callback: (String, String, Array[Byte]) => Unit): FilesVisitor = new FilesVisitor {
    override def requestOthers(category: String) = true
    override def visitOther(category: String, name: String, modified: FileTime): FromStore[Unit] = 
      { x => callback(category, name, x.data) }: FromStore.Binary[Unit]
  }

  // TODO--move this to an actual toSet
  object UnitTest {
    class ByteCounter extends mwt.utilities.FilesVisitor {
      var count: Long = 0L
      override def requestBlobs = true
      override def visitBlobData(name: String, modified: FileTime) = 
        { x => count += x.data.length; () }: FromStore.Binary[Unit]

      override def requestSummary = true
      override def visitSummary(name: String, modified: FileTime) = 
        { x => count += x.data.length; () }: FromStore.Binary[Unit]

      override def requestImages = true
      override def visitImage(name: String, modified: FileTime) = 
        { x => count += x.data.length; () }: FromStore.Binary[Unit]

      override def requestOthers(category: String) = true
      override def visitOther(category: String, name: String, modified: FileTime) = 
        { x => count += x.data.length; () }: FromStore.Binary[Unit]
    }

    // TODO--write a test!
  }
}

case class Contents[A](who: Path, target: OutputTarget, baseString: String, base: A, summary: Option[String], blobs: Array[String], images: Array[String], others: Map[String, Array[String]]) {
  def visitAll(fv: FilesVisitor): Ok[String, Unit] = safe {
    fv.start()
    if (target.isZip) {
      val zf = new ZipFile(who.toFile)
      try {
        val bSet = blobs.toSet
        val sSet = summary.toSet
        val iSet = images.toSet
        val oMap = others.toArray.flatMap{ case (k, vs) => vs.map(v => v -> k) }.toMap
        val (bzes, sze, izes, ozes) = {
          val bsb = Array.newBuilder[ZipEntry]
          val sb  = Array.newBuilder[ZipEntry]
          val isb = Array.newBuilder[ZipEntry]
          val osb = Array.newBuilder[(String, ZipEntry)]
          zf.entries.asScala.foreach{ e =>
            if (bSet contains e.getName)      bsb += e
            else if (iSet contains e.getName) isb += e
            else if (sSet contains e.getName) sb  += e
            else oMap.get(e.getName).foreach{ ext => osb += ext -> e }
          }
          (bsb.result, sb.result.headOption, isb.result, osb.result.groupBy(_._1).map{ case (k, vs) => k -> vs.map(_._2) })
        }
        if (fv.requestBlobs) {
          bzes.foreach{ bze =>
            fv.visitBlobData(bze.getName, bze.getLastModifiedTime) match {
              case b: FromStore.Binary[_] => b(zf.getInputStream(bze).gulp.?)
              case t: FromStore.Text[_]   => t(zf.getInputStream(bze).slurp.?)
            }
          }
          fv.noMoreBlobs()
        }
        if (fv.requestSummary) {
          sze match {
            case Some(ze) => fv.visitSummary(ze.getName, ze.getLastModifiedTime) match {
              case b: FromStore.Binary[_] => b(zf.getInputStream(ze).gulp.?)
              case t: FromStore.Text[_]   => t(zf.getInputStream(ze).slurp.?)
            }
            case None     => fv.noSummary()
          }
        }
        if (fv.requestImages) {
          izes.foreach{ ize =>
            fv.visitBlobData(ize.getName, ize.getLastModifiedTime) match {
              case b: FromStore.Binary[_] => b(zf.getInputStream(ize).gulp.?)
              case t: FromStore.Text[_]   => t(zf.getInputStream(ize).slurp.?)
            }
          }
        }
        ozes.foreach{ case (ext, zes) =>
          if (fv.requestOthers(ext) && zes.nonEmpty) {
            zes.foreach{ ze =>
              fv.visitOther(ext, ze.getName, ze.getLastModifiedTime) match {
               case b: FromStore.Binary[_] => b(zf.getInputStream(ze).gulp.?)
               case t: FromStore.Text[_]   => t(zf.getInputStream(ze).slurp.?)
              }
            }
            fv.noMoreOfTheseOthers()
          }
        }
      }
      finally { zf.close }
    }
    else {
      if (fv.requestBlobs) {
        blobs.foreach{ blob =>
          val p = who resolve blob
          fv.visitBlobData(blob, Files.getLastModifiedTime(p)) match {
            case b: FromStore.Binary[_] => b(p.toFile.gulp.?)
            case t: FromStore.Text[_]   => t(p.toFile.slurp.?)
          }
        }
        fv.noMoreBlobs()
      }
      if (fv.requestSummary) {
        summary match {
          case Some(s) =>
            val p = who resolve s
            fv.visitSummary(s, Files.getLastModifiedTime(p)) match {
              case b: FromStore.Binary[_] => b(p.toFile.gulp.?)
              case t: FromStore.Text[_]   => t(p.toFile.slurp.?)
            }
          case None =>
            fv.noSummary()
        }
      }
      if (fv.requestImages) {
        images.foreach{ image =>
          val p = who resolve image
          fv.visitImage(image, Files.getLastModifiedTime(p)) match {
            case b: FromStore.Binary[_] => b(p.toFile.gulp.?)
            case t: FromStore.Text[_]   => t(p.toFile.slurp.?)
          }
        }
        fv.noMoreImages()
      }
      others.foreach{ case (ext, fs) =>
        if (fv.requestOthers(ext) && fs.nonEmpty) {
          fs.foreach{ f =>
            val p = who resolve f
            fv.visitOther(ext, f, Files.getLastModifiedTime(p)) match {
              case b: FromStore.Binary[_] => b(p.toFile.gulp.?)
              case t: FromStore.Text[_]   => t(p.toFile.slurp.?)
            }
          }
          fv.noMoreOfTheseOthers()
        }
      }
    }
    fv.stop()
  }.mapNo(e => s"Could not visit MWT output\n${e.explain(24)}")

  /*
  def times(knownMax: Option[Double] = None, fromBlobs: Boolean = false): Ok[String, Array[Double]] =
    if (summary.isDefined && !fromBlobs) theSummary.map(_.times)
    else {
      // Timepoints we've witnessed (map to reported frame number and count delta)
      val m = collection.mutable.HashMap.empty[Double, (Mu[Int], Mu[Int])]

      // Inter-timepoint intervals we've witnessed (map to count of that interval)
      val dm = collection.mutable.HashMap.empty[Double, Mu[Int]]
      blobWalk(
        b => {
          var prev: Double = 0
          var t0: Double = Double.NaN
          b.foreach{ be =>
            if (prev > 0) {
              val delta = be.t - prev
              val x = dm.getOrElse(delta, Mu(0))
              x.value = x.value + 1
            }
            else t0 = be.t
            prev = be.t
            val (fr, n) = m.getOrElseUpdate(be.t, (Mu(0), Mu(0)))
            if (fr.value = 0) fr.value = be.frame
            else if (fr.value != be.frame) fr.value = -1
            n.value = n.value + 1
          }
          if (t0.finite) {
            val start = m(t0)
            start._2.value = start._2.value + 1
            val end = m(prev)
            end._2.value = end._2.value - 1
          }
        },
        false, false
      ).?

      // Find a reasonable lower bound time between frames
      val minDelta = dm.toArray.sortBy(_._1).pipe{ am =>
        var s = 0L; var i = 0; while (i < am.length) { s += am(i)._2.value; i += 1 }
        s /= 2
        i = 0; while (i < am.length && s > 0) { s -= am(i)._2.value; i += 1 }
        if (i >= am.length) 0.0 else Approximation.r5(0.1 * am(i)._1)
      }
      if (!minDelta.finite || minDelta < 1e-5) return No(s"Blob data has impossibly small time increments")

      // Sort by times
      val sorted = m.toArray.sortBy(_._1)

      // Remove overly close times (and mark that we aren't sure of the frame number)
      var i = 1
      var n = 0
      while (i < sorted.length) {
        if (sorted(i)._1 - sorted(n)._1 >= minDelta) {
          n += 1
          if (n < i) sorted(n) = sorted(i)
        }
        else {
          if (sorted(n)._2._1.value != sorted(i)._2._1.value) sorted(n)._2._1.value = -1
          sorted(n)._2._2.value = sorted(n)._2._2.value + sorted(i)._2._2.value
        }
        i += 1
      }
      if (n < sorted.length) n += 1

      // Make sure everything has enough space
      i = 0
      var j = 0
      var used = 0
      var fixed = 0
      while (i < n) {
        while (i < n && sorted(i)._2._1.value < 0) i += 1

        i += 1
      }

      // Figure out whether we can create new spots to fix any non-consecutive numbering
      var healable = false
      var atLeast
      var j = 0
      while (j < n && !healable) { if (sorted(j)._2 > 0) healable = true; j += 1 }
      if (healable && sorted(j)._2 < j) healable = false
      i = j+1
      /*
      while (i < n && healable) {
        if (sorted(i)._2 > 0) {
          val dt = sorted(i)._1 - sorted(j)._1
          val dn = sorted(i)._2 - sorted(j)._2
          if (dn < i - j) healable = false
          else if (minDelta > 0 && (dt/dn < 3*minDelta || dt/dn > 1000*minDelta)) healable = false
          else j = i
        }
        i += 1
      }
      */

      if (minDelta == 0 && !healable) No(s"Blob data inadequate to generate any reasonable summary")
      else if (minDelta == 0) {
        val ans = new Array[Double](n)
        var i = 0
        while (i < ans.length) {
          ans(i) = sorted(i)._1
          i += 1
        }
        Yes(ans)
      }
      else if (!healable) {
        val ansb = Array.newBuilder[Double]
        var i = 0
        var t = 0.0
        while (i < n) {
          if (sorted(i)._1 - t > 50*minDelta) {
            while (sorted(i)._1 - t > 15*minDelta) { t += 10*minDelta; ansb += Approximation.r5(t) }
          }
          ansb += sorted(i)._1
          i += 1
        }
        knownMax.foreach{ mt =>
          while (mt - t > 15*minDelta) { t += 10*minDelta; ansb += Approximation.r5(t) }
        }
        Yes(ansb.result)
      }
      else {
        val ansb = Array.newBuilder[Double]
        var i = 0
        var j = -1
        var t = 0.0
        var scratch = Contents.emptyDoubleArray
        while({while (i < n && sorted(i)._2 < 1) i += 1; i < n}) {
          val dt = sorted(i)._1 - t
          val dn = sorted(i)._2 - (if (j < 0) 0 else sorted(j)._2)
          if (dn == i - j) {
            while (j < i) { j += 1; ansb += sorted(j)._1 }
          }
          else if (i - j == 1) {
            var k = 1
            val inc = dt/dn
            while (k < dn) { ansb += Approximation.r5(t + k*inc); k += 1 }
            ansb += sorted(i)._1
            j = i
          }
          else {
            if (scratch.length < dn) scratch = new Array[Double](dn)
            scratch(dn - 1) = sorted(i)._1
            Contents.placeEvenly(scratch, t, 0, dn - 1, sorted, j+1, i-1)
            var k = 0
            while (k < dn) { ansb += scratch(k); k += 1 }
            j = i
          }
          t = sorted(i)._1
          i += 1
        }
        knownMax.foreach{ mt =>
          while (mt - t > 15*minDelta) { t += 10*minDelta; ansb += Approximation.r5(t) }
        }
        Yes(ansb.result)
      }
    }
  */

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

  def theSummary(): Ok[String, Summary] = summaryLines.flatMap(Summary from _.lines)


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

  def blobVisit(visitor: BlobVisitor): Ok[String, Unit] = {
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

  def blobLinesWalk[U](f: (Int, Stored.Text) => U) = blobVisit(new BlobVisitor {
    private var myId: Int = -1
    private var myVb: collection.mutable.Builder[String, Vector[String]] = null
    override def start(id: Int) = { myId = id; myVb = Vector.newBuilder[String]; true }
    def accept(line: String) = { myVb += line; true }
    override def stop() { f(myId, Stored.Text(myVb.result())); myVb = null }
  })

  def blobWalk[U](f: Blob => U, keepSkeleton: Boolean = true, keepOutline: Boolean = true): Ok[String, Unit] =
    blobLinesWalk((i, txt) => f(Blob.from(i, txt.lines, keepSkeleton, keepOutline).?))

  def theBlobs(keepSkeleton: Boolean = true, keepOutline: Boolean = true): Ok[String, Array[Blob]] = {
    val ab = Array.newBuilder[Blob]
    blobWalk(ab += _, keepSkeleton, keepOutline).map(_ => ab.result)
  }


  def imageWalk(dispatch: String => ImageAcceptor): Ok[String, Unit] = safe {
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

  def theImages(p: String => Boolean): Ok[String, Array[(String, java.awt.image.BufferedImage)]] = {
    val ans = Array.newBuilder[(String, java.awt.image.BufferedImage)]
    imageWalk{ filename =>
      if (!p(filename)) ImageDecoder.DoNotDecode
      val i = filename.lastIndexOf('.')
      val key = if (i < 0) "" else filename.substring(i)
      ImageDecoder.library.get(key).flatten match {
        case Some(d) => d.asAcceptor(img => ans += (filename -> img))
        case None    => ImageDecoder.DoNotDecode
      }
    }.map(_ => ans.result())
  }


  def otherWalk(interpreter: String => Option[String => FromStore[Unit]]): Ok[String, Unit] = safe {
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
  private class Bad(s: String) extends RuntimeException(s) {}
  private object Bad {
    def apply(s: String) = throw new Bad(s)
  }

  private[utilities] class TmFr(val time: Double, var frame: Int, var count: Int, var delta: Int)
  extends scala.math.Ordered[TmFr] {
    def framed = frame > 0

    def compare(that: TmFr): Int =
      if (time == that.time) 0
      else if (time.finite && time > that.time) 1
      else -1

    def register(fr: Int): this.type = {
      if (frame == 0) frame = fr
      else if (frame != fr) frame = -1
      count += 1
      this
    }

    def +=(that: TmFr): this.type = {
      if (frame == 0) frame = that.frame
      else if (frame != that.frame) frame = -1
      count += that.count
      delta += that.delta
      this
    }
  }
  private[utilities] object TmFr {
    def empty(time: Double) = new TmFr(time, 0, 0, 0)
  }

  private[utilities] val emptyDoubleArray = new Array[Double](0)

  private[utilities] def placeEvenly(target: Array[Double], t: Double, i0: Int, iN: Int, source: Array[(Double, Int)], j0: Int, j1: Int) {
    val tN = target(iN)
    val tStepN = 1 + iN - i0
    val tStep = (tN - t)/(tStepN max 1)
    if (tStep < 0) Bad(s"WTF?!  tStep=$tStep at $i0 $iN")
    if (j0 > j1) {
      // Fill in any missing spots evenly
      if (i0 < iN) {
        var k = i0
        while (k < iN) { target(k) = Approximation.r5(t + tStep*(k - i0 + 1)); k += 1 }
        if (target(iN-1) > target(iN)) Bad(s"WTF?!  Overshoot at ${iN-1}: ${target(iN-1)} to ${target(iN)}")
        if (i0 > 0 && (target(i0-1) > target(i0))) Bad(s"WTF?!  Undershoot at $i0: ${target(i0-1)} to ${target(i0)} maybe because $t")
      }      
    }
    else if (iN - i0 == 1 + j1 - j0) {
      // Same number in source and target, just copy over
      var i = i0
      var j = j0
      while (i < iN) {
        target(i) = source(j)._1
        i += 1
        j += 1
      }
      if (i0 > 0 && (target(i0-1) > target(i0))) Bad(s"WTF?!  Copy undershoot at $i0: ${target(i0-1)} to ${target(i0)}")
      if (iN > 0 && (target(iN-1) > target(iN))) Bad(s"WTF?!  Copy overshoot at ${iN-1}: ${target(iN-1)} to ${target(iN)}")
    }
    else {
      // Pick central-most source element, place, and recurse on either side
      val j = (j0 + j1) >>> 1
      val tj = source(j)._1
      var i = ((tj - t)/tStep).rint.toInt - 1
      if (i < j - j0) i = j-j0
      if (i > (iN - i0) - (j1 - j)) i = (iN - i0) - (j1 - j)
      i += i0
      if (i < i0) i = i0
      if (i >= iN) i = iN - 1
      target(i) = tj
      try {
        if (i0  < i)  placeEvenly(target, t,  i0,  i,  source, j0,  j-1)
      }
      catch {
        case b: Bad => Bad(s"Bad in leading range $i0 to $i with $t\n" + b.getMessage)
      }
      try {
        if (i+1 < iN) placeEvenly(target, tj, i+1, iN, source, j+1, j1)
      }
      catch {
        case b: Bad => Bad(s"Bad in trailing range ${i+1} to $iN with $tj\n" + b.getMessage)
      }
      if (i > 0 && target(i-1) > target(i)) Bad(s"Central target underflow at $i: ${target(i-1)} to ${target(i)}")
      if (i < iN && target(i) > target(i+1)) Bad(s"Central target overflow at $i: ${target(i)} to ${target(i+1)}")
    }
  }

  /*
  private[utilities] def groupConsecutive
  */

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

  object UnitTest {
    def test_place_evenly(): Ok[String, Unit] = {
      val r = new kse.maths.stochastic.Pcg64
      val target = new Array[Double](10)
      def reset() { for (i <- target.indices) target(i) = Double.NaN }

      reset()
      val source = new Array[(Double, Int)](10)
      for (i <- source.indices) source(i) = (2*(i+1) + r.D - 0.5, i+1)
      target(target.length-1) = source(source.length-1)._1
      placeEvenly(target, 0, 0, target.length-1, source, 0, source.length-2)
      for (i <- target.indices) if (target(i) != source(i)._1) return No(s"Same-length copy failed on index $i")

      reset()
      val src2 = source.zipWithIndex.collect{ case (e, i) if (i%2) == 1 => e }
      target(target.length-1) = src2(src2.length-1)._1
      placeEvenly(target, 0, 0, target.length-1, src2, 0, src2.length-2)
      for (i <- target.indices) {
        if ((i%2) == 1) { 
          if (target(i) != src2(i/2)._1) 
            return No(s"Half-length copy spread incorrectly on index $i\n  Full output: ${target.mkString(", ")}") 
        }
        else { 
          if ((target(i)-source(i)._1).abs > 1) 
            return No(s"Interpolation botched on index $i: found ${target(i)} but original was ${source(i)._1}\n  Full output: ${target.mkString(", ")}") 
        }
      }
      Ok.UnitYes
    }
  }
}
