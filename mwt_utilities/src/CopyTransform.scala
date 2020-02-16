package mwt.utilities

import java.io._
import java.nio.file._
import java.nio.file.attribute.FileTime
import java.nio.charset.StandardCharsets.UTF_8
import java.time._
import java.util.zip._

import scala.collection.JavaConverters._

import kse.flow._
import kse.coll._
import kse.eio._
import kse.maths._


/** This class is a visitor for MWT data that is used when copying-and-transforming directories.
  */
abstract class ContentTransformer[A] {
  /** An optional identifying title for the transform (used in error messages) */
  def title: String = ""

  /** This is called first; if it returns false, the copy/transform is not performed. */
  def start(everything: Contents[A]): Boolean = true

  /** Called to get the (potentially) new prefix before the date stamp in the directory or zip file name */
  def reprefix(prefix: Option[String]): Option[String] = prefix

  /** Called to (optionally) alter the base description string of files inside the archive */
  def rebase(base: String, parsed: A): (String, A) = (base, parsed)

  /** Called to figure out whether to put the target somewhere other than where it already is */
  def relocate(here: Path): Path = here

  /** Called to figure out whether to copy into a zip file or an uncompressed file */
  def toZip(): Boolean = true

  /** Specifies how blobs are to be treated.  If true, each blob will be visited and
    * may be modified.  Otherwise, they'll just be copied unchanged (and no blobs can
    * be added in that case).
    */
  def modifyBlobs(): Boolean = false

  /** This returns a handler for blob files.  Note that this will _only_ be called if `blobPolicy`
    * is `Modify`.  It either generates new text from the old one in a Some, or returns None if the
    * blob is to be removed.  The blob number will be ignored; all blobs get renumbered.
    */
  def blob(n: Int): Blob => Option[Blob] = b => Some(b)

  /** This is called when all blobs are done, but only if `modifyBlobs` is true.
    * The return is any new blobs that are to be created in addition to the old ones
    * (these blobs will get numbered automatically).
    */
  def stopBlobs(): Iterator[Blob] = Iterator.empty

  /** This is called when there is a summary file to get a transformer from old to new summary file contents.
    * Note that the data passed in here will have been generated from blob data if the summary file needed to
    * be generated, or if the blobs were changed/renumbered (and therefore the summary file needed modification too).
    * If `None`, the summary file will not be changed.
    */
  def summary(): Option[FromStore.Text.Transform] = None

  /** This is called on each image file to get a transformer from old to new image contents.
    * Image files will be deleted if a `None` is produced by the `FromStore`.
    * 
    * The return string specifies the new filename extension (if any).
    */
  def image(name: String): FromStore.Binary[Option[(String, Stored.Binary)]] = FromStore.Binary(x => Some((name.file.ext, x)))

  /** This is called after all existing image files have been seen.  The iterator can create new
    * image files.
    */
  def extraImages(): Iterator[(String, Stored.Binary)] = Iterator.empty

  /** This is called to handle each category of other kinds of file.  If `None` is given, then the files
    * are all skipped.  Otherwise, each filename is passed into the function to return an appropriate handler
    * that will pass along, regenerate, or delete (if `None` is returned) the data in that file.
    */
  def other(category: String): Option[String => FromStore[Option[Stored]]] = Some(_ => FromStore.Binary(x => Some(x)))

  /** This is called when all data has been seen.  The return specifies which additional files
    * to create, by "other" category number (i.e. file extension).  The returned Iterator is to
    * run through each file of that type.
    */
  def extraOther(): Map[String, Iterator[(String, Stored.Data)]] = Map.empty
}
object ContentTransformer {
  /** Copies existing files without changes */
  class Default[A]() extends ContentTransformer[A] {}

  /** Copies existing files without changes */
  def default[A]: ContentTransformer[A] = new Default[A]()


  trait TiffToPng[A] extends ContentTransformer[A] {
    override def image(name: String): FromStore.Binary[Option[(String, Stored.Binary)]] = {
      val sup = super.image(name)
      FromStore.Binary(bin => {
        sup(bin) match {
          case Some((ext, b)) if ext.equalsIgnoreCase("tiff") || ext.equalsIgnoreCase("tif") =>
            val pixels = ImageDecoder.tiffDecoder.decode(b.data).yesOr(e => throw new IOException(s"Couldn't decode tiff $name\n$e"))
            val baos = new ByteArrayOutputStream();
            val worked = javax.imageio.ImageIO.write(pixels, "png", baos)
            if (!worked) throw new IOException(s"Couldn't write content of $name into png")
            Some(("png", Stored.Binary(baos.toByteArray)))
          case x => x
        }
      })
    }
  }


  trait NoTiffs[A] extends ContentTransformer[A] {
    override def image(name: String): FromStore.Binary[Option[(String, Stored.Binary)]] = FromStore.Binary(x => None)
  }


  class Clean[A](val target: Path, val minMove: Double, val minTime: Double, val minFrames: Int = Summary.goodBlobN)
  extends ContentTransformer[A] {
    private def movedEnough(b: Blob): Boolean = {
      var eb = b(0)
      var minX = eb.cx
      var maxX = eb.cx
      var minY = eb.cy
      var maxY = eb.cy
      var maxL = eb.len
      var i = 1
      while (i < b.length) {
        eb = b(i)
        if      (eb.cx < minX) minX = eb.cx
        else if (eb.cx > maxX) maxX = eb.cx
        if      (eb.cy < minY) minY = eb.cy
        else if (eb.cy > maxY) maxY = eb.cy
        if (eb.len > maxL) maxL = eb.len
        i += 1
      }
      val ans = ((maxX - minX).sq + (maxY - minY).sq)/(if (maxL < 1) 1 else maxL).sq >= minMove.sq
      ans
    }
    override def relocate(here: Path) = target
    override def modifyBlobs() = true
    override def blob(n: Int) = blob => {
      if (blob.length == 0 || blob.length < minFrames) None
      else if (blob(blob.length-1).t - blob(0).t < minTime) None
      else if (!movedEnough(blob)) None
      else Some(blob)
    }    
  }

  /** Cleans up existing files without losing much */
  def clean[A](target: Path, minMove: Double, minTime: Double, minFrames: Int = Summary.goodBlobN) =
    new Clean[A](target, minMove, minTime, minFrames)

  /** Cleans and converts tiffs to PNGs */
  def nice[A](target: Path, minMove: Double, minTime: Double, minFrames: Int = Summary.goodBlobN) =
    new Clean[A](target, minMove, minTime, minFrames) with TiffToPng[A] {}

  /** Cleans and removes images */
  def bare[A](target: Path, minMove: Double, minTime: Double, minFrames: Int = Summary.goodBlobN) =
    new Clean[A](target, minMove, minTime, minFrames) with NoTiffs[A] {}
}


object CopyTransform {
  private def safeParent(path: Path): Path = {
    path.getParent                ReturnIf (_ ne null)
    path.toAbsolutePath.getParent ReturnIf (_ ne null)
    path.getFileSystem.getPath("")
  }

  def multiplex[A: scala.reflect.ClassTag](
    c: Contents[A], atomically: Boolean = true
  )(
    transforms: ContentTransformer[A]*
  ): Ok[String, Array[Option[Contents[A]]]] = {
    val results = new Array[Option[Contents[A]]](transforms.length)

    val (cts, oldixs) = transforms.toArray.zipWithIndex.filter{ case (ct, n) => 
      val y = ct.start(c)
      if (!y) results(n) = None
      y
    }.unzip

    val ctsi = cts.zipWithIndex

    if (results.forall(_ ne null)) return Yes(results)

    val prefixes = cts.map(ct => ct.reprefix(c.target.prefix))
    val (baseStrings, bases) = cts.map(ct => ct.rebase(c.baseString, c.base)).unzip
    val paths = cts.map(ct => ct.relocate(c.who |> safeParent))

    val zips = cts.map(ct => ct.toZip())
    val whoNames = c.who.getFileName.toString pipe { name =>
      val bits = name.split('_')
      val stamp = (if (bits.length <= 2) name else bits.takeRight(2).mkString("_"))
      ctsi.map{ case (ct, ix) =>
        val ans = prefixes(ix) match {
          case None    => stamp
          case Some(p) => p + "_" + stamp
        }
        val zip = zips(ix)
        if (zip == c.target.isZip) ans
        else if (zip)              ans + ".zip"
        else                       ans.dropRight(4)
      }
    }
    var tempNames = (if (atomically) whoNames.map(_ + ".atomic") else whoNames)

    val targets = (paths zip tempNames).map{ case (path, tempName) => path resolve tempName }
    var finalTargets = (paths zip whoNames).map{ case (path, whoName) => path resolve whoName }

    finalTargets.foreach{ finalTarget =>
      if (Files exists finalTarget)
        return No(s"Could not store ${c.who} because target exists:\n$finalTarget")
    }
    targets.foreach{ target =>
      if (atomically && Files.exists(target))
        return No(s"Could not store ${c.who} because temporary location exists:\n$target")
    }

    val mods = cts.map(ct => ct.modifyBlobs)

    val zosses: Array[ZipOutputStream] = ctsi.map{ case (_, ix) =>
      val target = targets(ix)
      safe {
        if (zips(ix)) (new ZipOutputStream(new FileOutputStream(target.toFile))).tap(_.setLevel(7))
        else (Files createDirectories target) pipe (_ => null)
      } TossAs (s"Could not write $target\n" + _.explain())
    }

    val someDataToWrite = Array.fill(cts.length)(true)
    val titles = cts.map(ct => ct.title)

    try {
      def titled(index: Int) = {
        if (index < 0 || index >= titles.length || titles(index).isEmpty) ""
        else s"While attempting copy ${titles(index)}:\n"
      }
      def newName(index: Int, name: String) = {
        val i = name.lastIndexOf(c.baseString)
        if (i < 0) throw new IllegalArgumentException(s"${titled(index)}Could not find ${c.baseString} in filename $name")
        baseStrings(index) + name.substring(i + c.baseString.length)
      }
      def bincopy(index: Int, name: String, modified: FileTime) = FromStore.Binary[Unit]{ b =>
        val zos = zosses(index)
        if (zos eq null) {
          val p = targets(index) resolve newName(index, name)
          Files.write(p, b.data)
          Files.setLastModifiedTime(p, modified)
        }
        else {
          val e = new ZipEntry(whoNames(index).dropRight(4) + "/" + newName(index, name))
          e.setLastModifiedTime(modified)
          zos.putNextEntry(e)
          zos.write(b.data)
          zos.closeEntry
        }
      }
      def txtcopy(index: Int, name: String, modified: FileTime) = FromStore.Text[Unit]{ txt =>
        val zos = zosses(index)
        if (zos eq null) {
          val p = targets(index) resolve newName(index, name)
          Files.write(p, txt.lines.asJava, UTF_8)
          Files.setLastModifiedTime(p, modified)
        }
        else {
          val e = new ZipEntry(whoNames(index).dropRight(4) + "/" + newName(index, name))
          e.setLastModifiedTime(modified)
          zos.putNextEntry(e)
          zos.write(txt.lines.mkString("\n").getBytes(UTF_8))
          zos.closeEntry
        }
      }

      ///// Start of MyVisitor
      abstract class MyVisitor extends FilesVisitor {
        override def requestImages = {
          val ans = someDataToWrite.exists(_ == true)
          ans
        }
        override def visitImage(name: String, modified: FileTime) = {
          FromStore.broadcast(
            ctsi.collect{ case (ct, ix) if someDataToWrite(ix) =>
              val imf = ct.image(name)
              FromStore.Binary{ bin =>
                imf(bin) match {
                  case Some((e, b)) =>
                    val copier = bincopy(ix, (name.file % e).toString, modified)
                    copier(b)
                  case _ => ()
                }
              }
            }
          )
        }
        override def noMoreImages() {
          for ((ct, ix) <- ctsi if someDataToWrite(ix)) {
            ct.extraImages().foreach{ case (name, bin) => bincopy(ix, name, FileTime from Instant.now).apply(bin) }            
          }
        }

        private[this] val myCategoryCaches = 
          Array.fill(cts.length)(collection.mutable.HashMap.empty[String, Option[String => FromStore[Option[Stored]]]])
        override def requestOthers(category: String) = someDataToWrite.exists(_ == true)
        override def visitOther(category: String, name: String, modified: FileTime) = FromStore.broadcast{ 
          ctsi.collect{ case (ct, ix) if someDataToWrite(ix) =>
            val myCategoryCache = myCategoryCaches(ix)
            myCategoryCache.getOrElseUpdate(category, ct.other(category)) match {
              case Some(f) => f(name) match {
                case fb: FromStore.Binary[Option[Stored]] => fb.branch(
                  os => os match {
                    case Some(b: Stored.Binary) => Left(b: Stored.Data)
                    case Some(t: Stored.Text)   => Left(t: Stored.Data)
                    case _                      => Right(())
                  },
                  bincopy(ix, name, modified),
                  txtcopy(ix, name, modified)
                )
                case ft: FromStore.Text[Option[Stored]]   => ft.branch(
                  os => os  match {
                    case Some(t: Stored.Text)   => Left(t: Stored.Data)
                    case Some(b: Stored.Binary) => Left(b: Stored.Data)
                    case _                      => Right(())
                  },
                  bincopy(ix, name, modified),
                  txtcopy(ix, name, modified)
                )
                case fd: FromStore.Duo[Option[Stored]]    => fd.branch(
                  os => os  match {
                    case Some(t: Stored.Text)   => Left(t: Stored.Data)
                    case Some(b: Stored.Binary) => Left(b: Stored.Data)
                    case _                      => Right(())
                  },
                  bincopy(ix, name, modified),
                  txtcopy(ix, name, modified)
                )
                case fe: FromStore.Empty[Option[Stored]]  => fe.asIfData(
                  os => os  match {
                    case Some(t: Stored.Text)   => Left(t: Stored.Data)
                    case Some(b: Stored.Binary) => Left(b: Stored.Data)
                    case _                      => Right(())
                  },
                  bincopy(ix, name, modified),
                  txtcopy(ix, name, modified)
                )
              }
              case _       => FromStore.Empty(_ => ())
            }
            bincopy(ix, name, modified)
          }
        }
        override def stop() {
          ctsi.foreach{ case (ct, ix) =>
            ct.extraOther().foreach{ case (category, others) =>
              others.foreach{ case (name, data) => data match { 
                case bin: Stored.Binary => bincopy(ix, name, FileTime from Instant.now).apply(bin)
                case txt: Stored.Text   => txtcopy(ix, name, FileTime from Instant.now).apply(txt)
              }}
            }
          }
        }
      }
      ///// End of MyVisitor

      
      if (mods.forall(_ == false) && c.summary.isDefined) c.visitAll(new MyVisitor {
        private[this] var foundBlobs = Array.fill(cts.length)(false)

        override def requestBlobs = true
        override def visitBlobData(name: String, modified: FileTime) =  FromStore.broadcastBinary{
          ctsi.map{ case (ct, ix) =>
            foundBlobs(ix) = true
            bincopy(ix, name, modified)
          }
        }
        override def noMoreBlobs() {
          for (i <- someDataToWrite.indices) someDataToWrite(i) = foundBlobs(i)
        }

        override def requestSummary = someDataToWrite.exists(_ == true)
        override def visitSummary(name: String, modified: FileTime) = FromStore.broadcast{ ctsi.map{ case (ct, ix) =>
          ct.summary() match {
            case Some(xf) => xf andThen txtcopy(ix, name, modified)
            case _        => bincopy(ix, name, modified)
          }
        }}
      }).?
      else {
        val moai: Mu[Option[Array[Int]]] = Mu(None)  // TODO--can we use this?  Or should we drop it?
        val moadl: Mu[Option[Array[(Double, Long)]]] = Mu(None)  // Any point using this?
        val ts = c.times(storeStimuli = Some(moadl), storeTransitions = Some(moai)).yesOr(no => throw new IOException(no))
        val sms = Array.fill(cts.length)(new Summary())
        sms.foreach{ sm =>
          ts.foreach(t => sm add Summary.Entry(t))
        }
        val seens = Array.fill(cts.length)(collection.mutable.Set.empty[Int])
        val mod = FileTime from Instant.now

        val currentBlocks = Array.fill(cts.length)(0)
        var currentFills = Array.fill(cts.length)(0)
        def currentBlobsName(index: Int) = f"${c.baseString}_${currentBlocks(index)}%05dk.blobs"
        val currentTexts = Array.fill(cts.length)(Vector.newBuilder[String])
        def writeBlobsAndAdvance(index: Int) {
          if (currentFills(index) > 0) {
            txtcopy(index, currentBlobsName(index), mod).apply(Stored.Text(currentTexts(index).result))
            currentBlocks(index) += 1
            currentFills(index) = 0
            currentTexts(index) = Vector.newBuilder[String]
          }
        }
        def writeOneBlob(index: Int, b: Blob, sm: Summary) {
          currentFills(index) += 1
          currentTexts(index) += s"% ${currentBlocks(index)*1000 + currentFills(index)}"
          currentTexts(index) ++= b.text(sm)
          if (currentFills(index) == 1000) writeBlobsAndAdvance(index)
        }

        def writeSummary(index: Int) {
          txtcopy(index, s"${c.baseString}.summary", mod).apply(Stored.Text(sms(index).text()))
        }

        c.visitAll(new MyVisitor {
          private def handleOneBlob(index: Int, b: Blob, id: Int) {
            cts(index).blob(id).apply(b) match {
              case Some(x) =>
                sms(index) imprint x
                writeOneBlob(index, x, sms(index))
              case None => ()
            }
          }

          override def requestBlobs = true
          override def visitBlobData(name: String, modified: FileTime): FromStore.Text[Unit] = txt => {
            if (name.endsWith("blob")) {
              ctsi.foreach{ case (ct, ix) =>
                val id = c.getIdFromBlobName(name, seens(ix)).yesOr(no => throw new IOException(no))
                val b = Blob.from(id, txt.lines, keepSkeleton = false, keepOutline = true).yesOr(no => throw new IOException(no))
                handleOneBlob(ix, b, id)
              }
            }
            else {
              var i = 0
              while (i < txt.lines.length && !txt.lines(i).startsWith("%")) i += 1
              while (i < txt.lines.length) {
                val ids = ctsi.map{ case (ct, ix) => 
                  c.getIdFromBlobsLine(txt.lines(i), name, i+1, seens(ix)).yesOr(no => throw new IOException(no))
                }
                var j = i + 1
                while (j < txt.lines.length && !txt.lines(j).startsWith("%")) j += 1
                val b = Blob.
                  from(0, txt.lines.slice(i+1, j), keepSkeleton = false, keepOutline = true).
                  yesOr(no => throw new IOException(no))
                ctsi.foreach{ case (ct, ix) => handleOneBlob(ix, b, ids(ix)) }
                i = j
              }
            }
          }
          override def noMoreBlobs() {
            ctsi.foreach{ case (ct, ix) =>
              someDataToWrite(ix) = currentBlocks(ix) > 0 || currentFills(ix) > 0
              ct.stopBlobs().foreach{ blob => writeOneBlob(ix, blob, sms(ix)) }
              writeBlobsAndAdvance(ix)
            }
          }

          override def requestSummary = someDataToWrite.exists(_ == true)
          override def visitSummary(name: String, modified: FileTime): FromStore.Text[Unit] = txt => {
            val orig = Summary.from(txt.lines).yesOr(no => throw new IOException(no))
            ctsi.foreach{ case (ct, ix) =>
              sms(ix) adoptEvents orig
              writeSummary(ix)
            }
          }
          override def noSummary() {
            ctsi.foreach{ case (ct, ix) => writeSummary(ix) }
          }
        }).?
      }
    }
    finally {
      var i = 0
      while (i < zosses.length) {
        val zos = zosses(i)
        if (zos ne null) zos.close
        i += 1
      }
    }

    ctsi.foreach{ case (ct, ix) =>
      results(ix) =
        if (!someDataToWrite(ix))
          safe{ Files delete targets(ix) }.mapNo(e => s"Could not delete empty ${targets(ix)}\n${e.explain()}").map(_ => None).?
        else {
          if (atomically) Files.move(targets(ix), finalTargets(ix), StandardCopyOption.ATOMIC_MOVE)
          Contents.from[A](finalTargets(ix), _ => Yes(bases(ix))).map(y => Some(y)).?
        }
    }
    Yes(results)
  }

  /** Returns Yes(None) if the file isn't supposed to be copied (e.g. because it's empty); Yes(Contents)
    * if the copying went properly; and `No(msg)` if something went wrong.
    */
  def apply[A: scala.reflect.ClassTag](
    c: Contents[A],
    ct: ContentTransformer[A] = ContentTransformer.default[A],
    atomically: Boolean = true
  ): Ok[String, Option[Contents[A]]] =
    multiplex(c, atomically)(ct).map(_.head)
}
