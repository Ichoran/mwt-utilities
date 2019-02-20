package mwt.utilities

import java.io._
import java.nio.file._
import java.nio.file.attribute.FileTime
import java.time._
import java.util.zip._

import scala.collection.JavaConverters._

import kse.flow._
import kse.coll._
import kse.eio._
import kse.maths._
import kse.maths.stats._


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

  // TODO--move this to an actual unit test
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

    // TODO--write more tests!
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
              case d: FromStore.Duo[_]    => zf.getInputStream(bze).gulp.? pipe (b => d((new String(b)).linesIterator.toVector, b))
              case e: FromStore.Empty[_]  => e()
            }
          }
          fv.noMoreBlobs()
        }
        if (fv.requestSummary) {
          sze match {
            case Some(ze) => fv.visitSummary(ze.getName, ze.getLastModifiedTime) match {
              case b: FromStore.Binary[_] => b(zf.getInputStream(ze).gulp.?)
              case t: FromStore.Text[_]   => t(zf.getInputStream(ze).slurp.?)
              case d: FromStore.Duo[_]    => zf.getInputStream(ze).gulp.? pipe (b => d((new String(b)).linesIterator.toVector, b))
              case e: FromStore.Empty[_]  => e()
            }
            case None     => fv.noSummary()
          }
        }
        if (fv.requestImages) {
          izes.foreach{ ize =>
            fv.visitBlobData(ize.getName, ize.getLastModifiedTime) match {
              case b: FromStore.Binary[_] => b(zf.getInputStream(ize).gulp.?)
              case t: FromStore.Text[_]   => t(zf.getInputStream(ize).slurp.?)
              case d: FromStore.Duo[_]    => zf.getInputStream(ize).gulp.? pipe (b => d((new String(b)).linesIterator.toVector, b))
              case e: FromStore.Empty[_]  => e()
            }
          }
        }
        ozes.foreach{ case (ext, zes) =>
          if (fv.requestOthers(ext) && zes.nonEmpty) {
            zes.foreach{ ze =>
              fv.visitOther(ext, ze.getName, ze.getLastModifiedTime) match {
                case b: FromStore.Binary[_] => b(zf.getInputStream(ze).gulp.?)
                case t: FromStore.Text[_]   => t(zf.getInputStream(ze).slurp.?)
                case d: FromStore.Duo[_]    => zf.getInputStream(ze).gulp.? pipe (b => d((new String(b)).linesIterator.toVector, b))
                case e: FromStore.Empty[_]  => e()
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
            case d: FromStore.Duo[_]    => p.toFile.gulp.? pipe (b => d((new String(b)).linesIterator.toVector, b))
            case e: FromStore.Empty[_]  => e()
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
              case d: FromStore.Duo[_]    => p.toFile.gulp.? pipe (b => d((new String(b)).linesIterator.toVector, b))
              case e: FromStore.Empty[_]  => e()
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
            case d: FromStore.Duo[_]    => p.toFile.gulp.? pipe (b => d((new String(b)).linesIterator.toVector, b))
            case e: FromStore.Empty[_]  => e()
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
              case d: FromStore.Duo[_]    => p.toFile.gulp.? pipe (b => d((new String(b)).linesIterator.toVector, b))
              case e: FromStore.Empty[_]  => e()
            }
          }
          fv.noMoreOfTheseOthers()
        }
      }
    }
    fv.stop()
  }.mapNo(e => s"Could not visit MWT output\n${e.explain(24)}")

  
  def times(
    knownMax: Option[Double] = None,
    fromBlobs: Boolean = false,
    storeStimuli: Option[Mu[Option[Array[(Double, Long)]]]],
    storeTransitions: Option[Mu[Option[Array[Int]]]] = None
  ): Ok[String, Array[Double]] =
    if (summary.isDefined && !fromBlobs) theSummary.map{ s =>
      storeStimuli.foreach{ madl =>
        val st = Array.newBuilder[(Double, Long)]
        var i = 0
        while (i < s.length) {
          val e = s(i)
          if (e.stimuli != 0L) st += ((e.t, e.stimuli))
          i += 1
        }
        madl.value = Some(st.result)
      }
      storeTransitions.foreach{ mai =>
        val tr = Array.newBuilder[Int]
        var i = 0
        while (i < s.length) {
          val e = s(i)
          if (e.findLoss ne null) {
            val fl = e.findLoss
            var j = 0
            while (j + 1 < fl.length) {
              if (fl(j) > 0 && fl(j+1) > 0) {
                tr += i + 1
                tr += fl(j)
                tr += fl(j+1)
              }
              j += 2
            }
          }
          i += 1
        }
        mai.value = Some(tr.result)
      }
      s.times
    }
    else {
      import Contents.TmFr

      storeTransitions.foreach{ mai => mai.value = None }
      storeStimuli.foreach{ mai => mai.value = None }

      // Timepoints we've witnessed (map to reported frame number and count delta)
      val m = collection.mutable.HashMap.empty[Double, TmFr]

      // Inter-timepoint intervals we've witnessed (map to count of that interval)
      val dm = collection.mutable.HashMap.empty[Double, Mu[Int]]
      blobWalk(
        b => {
          var prev: Double = Double.NegativeInfinity
          var t0: Double = Double.NegativeInfinity
          b.foreach{ be =>
            if (prev > 0) {
              val delta = be.t - prev
              val x = dm.getOrElseUpdate(delta, Mu(0))
              x.value = x.value + 1
            }
            else t0 = be.t
            prev = be.t
            m.getOrElseUpdate(be.t, TmFr.empty(be.t)).register(be.frame)
          }
          m.get(t0).foreach(_.++)
          m.get(prev).foreach(_.--)
        },
        false, false
      ).?

      if (m.size == 0) return Yes(Contents.emptyDoubleArray)

      // Find a reasonable lower bound time between frames
      val minDelta = dm.toArray.sortBy(_._1).pipe{ am =>
        var s = 0L; var i = 0; while (i < am.length) { s += am(i)._2.value; i += 1 }
        s /= 2
        i = 0; while (i < am.length && s > 0) { s -= am(i)._2.value; i += 1 }
        if (i >= am.length) 0.0 else Approximation.r5(0.1 * am(i)._1)
      }
      if (!minDelta.finite || minDelta < 1e-5) return No(s"Blob data has impossibly small time increments")

      // Break apart into blocks of known sequential frames
      val chunked = TmFr.fragment( TmFr.mergeClose(m.toArray.map(_._2).sorted, minDelta) ).pipe{ ch =>
        knownMax match {
          case Some(t) if (t - ch.last.last.time > 100*minDelta) => ch :+ Array(new TmFr(t, -1, 0, 0))
          case _ => ch
        }
      }

      val flat = new Array[Double](chunked.last.last.frame)
      var k = 0
      var t = 0.0
      var i = 0
      while (i < chunked.length) {
        val chi = chunked(i)
        if (chi.head.frame - 1 > k) {
          val dt = (chi.head.time - t)/(chi.head.frame - k)
          while (k < chi.head.frame - 1) {
            flat(k) = Approximation.r5(t + dt)
            t += dt
            k += 1
          }
        }
        var j = 0
        while (j < chi.length) {
          flat(k) = chi(j).time
          j += 1
          k += 1
        }
        i += 1
      }
      Yes(flat)
    }


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
        blobs.foreach{ blobf =>
          if (blobf endsWith ".blob") {
            if (visitor.start(getIdFromBlobName(blobf, seen).?)) {
              Files.lines(who resolve blobf).forEach(visitor accept _)
              visitor.stop()
            }
          }
          else {
            var n = 0
            var id = 0
            var visit = false
            Files.lines(who resolve blobf).forEach{ line =>
              n += 1
              if (line.length > 0 && line.charAt(0) == '%') {
                if (visit) visitor.stop()
                id = getIdFromBlobsLine(line, blobf, n, seen).?
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
              case fb: FromStore.Binary[Unit]  => fb(zf.getInputStream(ze).gulp.?)
              case ft: FromStore.Text[Unit]    => ft(zf.getInputStream(ze).slurp.?)
              case fd: FromStore.Duo[Unit]     => zf.getInputStream(ze).gulp.? pipe (b => fd((new String(b)).linesIterator.toVector, b))
              case fe: FromStore.Empty[Unit]   => fe()
            }
          }
        }
        else {
          aFor(files){ (file, i) => acceptor(file) match {
            case fb: FromStore.Binary[Unit] => fb(Files.readAllBytes(who resolve file))
            case ft: FromStore.Text[Unit]   =>
              val vb = Vector.newBuilder[String]
              Files.lines(who resolve file).forEach(vb += _)
              ft(vb.result)
            case fd: FromStore.Duo[Unit]    => Files.readAllBytes(who resolve file) pipe (b => fd((new String(b)).linesIterator.toVector, b))
            case fe: FromStore.Empty[Unit]  => fe()
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
  extends scala.math.Ordered[TmFr] with Cloneable {
    def framed = frame > 0

    def compare(that: TmFr): Int =
      if (time == that.time) 0
      else if (time.finite && time > that.time) 1
      else -1

    def --(): this.type = { delta -= 1; this }
    def ++(): this.type = { delta += 1; this }

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

    override def clone(): TmFr = new TmFr(time, frame, count, delta)

    override def toString: String = s"$time/$frame/$count/$delta"

    override def equals(a: Any) = a match {
      case that: TmFr => time == that.time && frame == that.frame && count == that.count && delta == that.delta
      case _ => false
    }
  }
  private[utilities] object TmFr {
    def apply(time: Double, frame: Int, count: Int, delta: Int) = new TmFr(time, frame, count, delta)

    def empty(time: Double) = new TmFr(time, 0, 0, 0)

    def fragment(xs: Array[TmFr]): Array[Array[TmFr]] = {
      val xssb = Array.newBuilder[Array[TmFr]]
      var i = 0
      while (i < xs.length) {
        var cuml = xs(i).delta
        var j = i+1
        while (cuml > 0 && j < xs.length) {
          cuml += xs(j).delta
          j += 1
        }
        xssb += java.util.Arrays.copyOfRange(xs, i, j)
        i = j
      }
      xssb.result
    }

    def distribute(ts: Array[Double], n: Int): Array[Int] = {
      var tsum = 0.0
      var i = 0
      while (i < ts.length) { tsum += ts(i); i += 1 }
      var nsum = n
      val ns = new Array[Int](ts.length)
      i = 0
      while (i < ts.length) {
        val frac = if (tsum == 0) 1.0 else ts(i)/tsum
        tsum -= ts(i)
        ns(i) = (n*frac).rint.toInt.clip(1, nsum - (ns.length - i - 1))
        nsum -= ns(i)
        i += 1 
      }
      if (nsum > 0) ns(ns.length-1) += nsum
      ns
    }

    def unifyInPlace(xs: Array[TmFr], lowestPossibleFrame: Int = 1) {
      val e = EstM()
      var i = 0
      while (i < xs.length) { if (xs(i).framed) e += xs(i).frame - i; i += 1 }
      val zero = (if (e.n > 0) e.mean.rint.toInt else 1) max lowestPossibleFrame
      i = 0
      while (i < xs.length) { 
        xs(i).frame = zero + i
        i += 1
      }
    }

    def unifyInPlace(xss: Array[Array[TmFr]], delta: Double): Ok[String, Unit] = {
      var i = 0
      var minFr = 1
      while (i < xss.length) {
        var framed = false
        val xsi = xss(i)
        var k = 0
        while (!framed && k < xsi.length) { framed = xsi(k).framed; k += 1 }
        if (framed) {
          unifyInPlace(xsi, lowestPossibleFrame = minFr)
          minFr = xsi.last.frame + 1
          var j = i-1
          var nFr = 0
          while (j >= 0 && !xss(j).head.framed) { nFr += xss(j).length; j -= 1 }
          if (i-j > 1) {
            val gapT = {
              val ans = new Array[Double](i-j)
              var k = j
              while (k < i) {
                ans(k - j) = xss(j+1).head.time - (if (j >= 0) xss(j).last.time else 0.0)
                if (ans(k-j) <= 0) return No(s"Blobs have negative time interval around ${xss(j+1).head.time}")
                k += 1
              }
              ans
            }
            val nUnassigned = xss(i).head.frame - (if (j >= 0) xss(j).last.frame else 0)
            val gapN = distribute(gapT, nUnassigned - nFr + gapT.length - 1)
            k = j+1
            while (k < i) {
              val nStart = (if (k > 0) xss(k-1).last.frame else 0) + gapN(k-j-1)
              unifyInPlace(xss(k), lowestPossibleFrame = nStart)
              k += 1
            }
          }
        }
        else minFr += xsi.length
        i += 1
      }
      if (xss.length > 0 && !xss.last.head.framed) {
        i = xss.length-1
        while (i >= 0 && !xss(i).head.framed) i -= 1
        var lastN = (if (i >= 0) xss(i).last.frame else 0)
        var lastT = (if (i >= 1) xss(i).last.time  else 0)
        i += 1
        while (i < xss.length) {
          val xsi = xss(i)
          val dt = xsi.head.time - lastT
          val dn = (if (delta > 0) (dt/delta).floor.toInt else 1) max 1
          unifyInPlace(xsi, lowestPossibleFrame = lastN + dn)
          lastN = xsi.last.frame
          lastT = xsi.last.time
          i += 1
        }
      }
      Ok.UnitYes
    }

    def mergeClose(xs: Array[TmFr], minDelta: Double): Array[TmFr] = {
      // Remove overly close times (and mark that we aren't sure of the frame number)
      var i = 1
      var n = 0
      while (i < xs.length) {
        if (xs(i).time - xs(n).time >= minDelta) {
          n += 1
          if (n < i) xs(n) = xs(i)
        }
        else xs(n) += xs(i)
        i += 1
      }
      if (n < xs.length) n += 1
      if (n < xs.length) java.util.Arrays.copyOf(xs, n) else xs
    }


    object UnitTest {
      def test_distribute(): Ok[String, Unit] = {
        distribute(Array(1, 1, 1, 1, 1), 5).toVector.okIf(_ == Vector(1, 1, 1, 1, 1)) TossAs (v => s"Not 1,1,1,1,1: $v")
        distribute(Array(1, 5), 3).toVector.okIf(_ == Vector(1, 2)) TossAs (v => s"Not 1,2: $v")
        distribute(Array(9, 1), 3).toVector.okIf(_ == Vector(2, 1)) TossAs (v => s"Not 2,1: $v")
        distribute(Array(1, 1.1), 3).toVector.okIf(_ == Vector(1, 2)) TossAs (v => s"Not 1,2: $v")
        distribute(Array(1.1, 1), 3).toVector.okIf(_ == Vector(2, 1)) TossAs (v => s"Not 2,1: $v")
        Ok.UnitYes
      }
      def test_fragment(): Ok[String, Unit] = {
        val source = Array(
          TmFr(1, 16, 1, 1), TmFr(2, 17, 1, 0), TmFr(3, 18, 1, -1),
          TmFr(5, 20, 2, 0),
          TmFr(7, 22, 1, 1), TmFr(8, 23, 2, 1), TmFr(9, 24, 3, 1), TmFr(10, 25, 4, -3),
          TmFr(12, 27, 1, 0)
        )
        val frag = fragment(source)
        lazy val frs = frag.map(_.mkString(", ")).mkString("\n")
        if (frag.length != 4)
          No(s"Should have had 4 lines, but was\n$frs")
        else if (frag.map(_.length).toVector != Vector(3, 1, 4, 1)) 
          No(s"Should have had lengths 3, 1, 4, 1, but was\n$frs")
        else if (frag.flatten.map(_.frame).toVector != Vector(16, 17, 18, 20, 22, 23, 24, 25, 27))
          No(s"Should have had frames\n16 17 18\n20\n22 23 24 25\n27, but was\n$frs")
        else Ok.UnitYes
      }
      def test_unifyInPlace(): Ok[String, Unit] = {
        val source = Array(
          Array(TmFr(6, -1, 1, 0)),
          Array(TmFr(11, 16, 1, 1), TmFr(12, 17, 1, 0), TmFr(13, 18, 1, -1)),
          Array(TmFr(15, 20, 2, 0)),
          Array(TmFr(17, -1, 1, 1), TmFr(18, -1, 2, 1), TmFr(19, -1, 3, 1), TmFr(20, -1, 4, -3)),
          Array(TmFr(22, 27, 1, 0)),
          Array(TmFr(24, -1, 2, 0))
        )
        val fixed = source.map(si => si.map(_.clone))
        lazy val fxs = fixed.map(_.mkString(" ")).mkString("\n")
        unifyInPlace(fixed, 1)
        (source zip fixed).okIf(_.forall(sf => sf._1.length == sf._2.length)) TossAs (_ => s"Changed structure:\n$fxs")
        (source(1) zip fixed(1)).okIf(_.forall(sf => sf._1 == sf._2)) TossAs (_ => s"Changed second line:\n$fxs")
        (source(2) zip fixed(2)).okIf(_.forall(sf => sf._1 == sf._2)) TossAs (_ => s"Changed third line:\n$fxs")
        (source(4) zip fixed(4)).okIf(_.forall(sf => sf._1 == sf._2)) TossAs (_ => s"Changed fifth line:\n$fxs")
        fixed(0)(0).okIf(_.frame in (7, 9)) TossAs (x => s"Frame out of range in $x; overall:\n$fxs")
        fixed(3).okIf(_.map(_.frame).toVector == (22 to 25)) TossAs (x => s"Frames not in 22 to 25 in ${x.mkString(" ")}; overall:\n$fxs")
        fixed(5)(0).okIf(_.frame == 29) TossAs (x => s"Frame not 29 in $x; overall:\n$fxs")
        Ok.UnitYes
      }
      def test_all(): Ok[Vector[String], Unit] = {
        val errors = Vector.newBuilder[String]
        test_distribute().foreachNo(errors += _)
        test_fragment().foreachNo(errors += _)
        test_unifyInPlace().foreachNo(errors += _)
        errors.result.okIf(_.isEmpty).map(_ => ())
      }
    }
  }

  private[utilities] val emptyDoubleArray = new Array[Double](0)


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

  def from[A](p: Path, parser: String => Ok[String, A] = (s: String) => Ok.UnitYes): Ok[String, Contents[A]] = {
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
      def namedLikeMwtTarget: Boolean = {
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
    def test_TmFr(): Ok[String, Unit] = TmFr.UnitTest.test_all().mapNo(_.mkString("TmFr tests:\n", "\n---\n", "\nEnd TmFr\n\n"))
    def test_all(): Ok[Vector[String], Unit] = {
      val errors = Vector.newBuilder[String]
      test_TmFr().foreachNo(errors += _)
      errors.result.okIf(_.isEmpty).map(_ => ())
    }
  }
}
