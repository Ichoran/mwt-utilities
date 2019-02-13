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
class ContentsTransformer[A] {
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
    * Image files can be deleted a `None` is produced by the `FromStore`.
    */
  def image(name: String): FromStore.Binary[Option[Stored.Binary]] = FromStore.Binary(x => Some(x))

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
object ContentsTransformer {
  /** Copies existing files without changes */
  class Default[A]() extends ContentsTransformer[A] {}

  /** Copies existing files without changes */
  def default[A]: ContentsTransformer[A] = new Default[A]()


  class Clean[A](val target: Path, val minMove: Double, val minTime: Double, val minFrames: Int = Summary.goodBlobN)
  extends ContentsTransformer[A] {
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
}


object CopyTransform {
  private def safeParent(path: Path): Path = {
    path.getParent                ReturnIf (_ ne null)
    path.toAbsolutePath.getParent ReturnIf (_ ne null)
    path.getFileSystem.getPath("")
  }

  /** Returns Yes(None) if the file isn't supposed to be copied (e.g. because it's empty); Yes(Contents)
    * if the copying went properly; and `No(msg)` if something went wrong.
    */
  def apply[A](
    c: Contents[A],
    ct: ContentsTransformer[A] = ContentsTransformer.default[A],
    atomically: Boolean = true
  ): Ok[String, Option[Contents[A]]] =
  {
    if (!ct.start(c)) return Yes(None)

    val prefix = ct.reprefix(c.target.prefix)
    val (baseString, base) = ct.rebase(c.baseString, c.base)
    val path = ct.relocate(c.who |> safeParent)

    val zip = ct.toZip()
    val whoName = c.who.getFileName.toString pipe { name =>
      val bits = name.split('_')
      val stamp = (if (bits.length <= 2) name else bits.takeRight(2).mkString("_"))
      val ans = prefix match {
        case None    => stamp
        case Some(p) => p + "_" + stamp
      }
      if (zip == c.target.isZip) ans
      else if (zip)              ans + ".zip"
      else                       ans.dropRight(4)
    }
    var tempName = (if (atomically) whoName + ".atomic" else whoName)

    val target = path resolve tempName
    var finalTarget = path resolve whoName

    if (Files exists finalTarget) return No(s"Could not store ${c.who} because target exists:\n$finalTarget")
    if (atomically && Files.exists(target)) return No(s"Could not store ${c.who} because temporary location exists:\n$target")

    val mod = ct.modifyBlobs

    val zos: ZipOutputStream = safe {
      if (zip) (new ZipOutputStream(new FileOutputStream(target.toFile))).tap(_.setLevel(7))
      else (Files createDirectories target) pipe (_ => null)
    } TossAs (s"Could not write $target\n" + _.explain())

    var someDataToWrite = true

    try {
      def newName(name: String) = {
        val i = name.lastIndexOf(c.baseString)
        if (i < 0) throw new IllegalArgumentException(s"Could not find ${c.baseString} in filename $name")
        baseString + name.substring(i + c.baseString.length)
      }
      def bincopy(name: String, modified: FileTime) = FromStore.Binary[Unit]{ b =>
        if (zos eq null) {
          val p = target resolve newName(name)
          Files.write(p, b.data)
          Files.setLastModifiedTime(p, modified)
        }
        else {
          val e = new ZipEntry(whoName.dropRight(4) + "/" + newName(name))
          e.setLastModifiedTime(modified)
          zos.putNextEntry(e)
          zos.write(b.data)
          zos.closeEntry
        }
      }
      def txtcopy(name: String, modified: FileTime) = FromStore.Text[Unit]{ txt =>
        if (zos eq null) {
          val p = target resolve newName(name)
          Files.write(p, txt.lines.asJava, UTF_8)
          Files.setLastModifiedTime(p, modified)
        }
        else {
          val e = new ZipEntry(whoName.dropRight(4) + "/" + newName(name))
          e.setLastModifiedTime(modified)
          zos.putNextEntry(e)
          zos.write(txt.lines.mkString("\n").getBytes(UTF_8))
          zos.closeEntry
        }
      }

      ///// Start of MyVisitor
      abstract class MyVisitor extends FilesVisitor {
        override def requestImages = someDataToWrite
        override def visitImage(name: String, modified: FileTime) = ct.image(name).
          branch(_.toOk.swap.toEither, bincopy(name, modified))
        override def noMoreImages() {
          ct.extraImages().foreach{ case (name, bin) => bincopy(name, FileTime from Instant.now).apply(bin) }
        }

        private[this] val myCategoryCache = collection.mutable.HashMap.empty[String, Option[String => FromStore[Option[Stored]]]]
        override def requestOthers(category: String) = someDataToWrite
        override def visitOther(category: String, name: String, modified: FileTime) = {
          myCategoryCache.getOrElseUpdate(category, ct.other(category)) match {
            case Some(f) => f(name) match {
              case fb: FromStore.Binary[Option[Stored]] => fb.branch(
                os => os match {
                  case Some(b: Stored.Binary) => Left(b: Stored.Data)
                  case Some(t: Stored.Text)   => Left(t: Stored.Data)
                  case _                      => Right(())
                },
                bincopy(name, modified),
                txtcopy(name, modified)
              )
              case ft: FromStore.Text[Option[Stored]]   => ft.branch(
                os => os  match {
                  case Some(t: Stored.Text)   => Left(t: Stored.Data)
                  case Some(b: Stored.Binary) => Left(b: Stored.Data)
                  case _                      => Right(())
                },
                txtcopy(name, modified),
                bincopy(name, modified)
              )
              case fe: FromStore.Empty[Option[Stored]]  => fe.asIfData(
                os => os  match {
                  case Some(t: Stored.Text)   => Left(t: Stored.Data)
                  case Some(b: Stored.Binary) => Left(b: Stored.Data)
                  case _                      => Right(())
                },
                txtcopy(name, modified),
                bincopy(name, modified)
              )
            }
            case _       => FromStore.Empty(_ => ())
          }
          bincopy(name, modified)
        }
        override def stop() {
          ct.extraOther().foreach{ case (category, others) =>
            others.foreach{ case (name, data) => data match { 
              case bin: Stored.Binary => bincopy(name, FileTime from Instant.now).apply(bin)
              case txt: Stored.Text   => txtcopy(name, FileTime from Instant.now).apply(txt)
            }}
          }
        }
      }
      ///// End of MyVisitor

      
      if (!mod && c.summary.isDefined) c.visitAll(new MyVisitor {
        private[this] var foundBlobs = false

        override def requestBlobs = true
        override def visitBlobData(name: String, modified: FileTime) = {
          foundBlobs = true
          bincopy(name, modified)
        }
        override def noMoreBlobs() {
          someDataToWrite = foundBlobs
        }

        override def requestSummary = someDataToWrite
        override def visitSummary(name: String, modified: FileTime) = ct.summary() match {
          case Some(xf) => xf andThen txtcopy(name, modified)
          case _        => bincopy(name, modified)
        }
      }).?
      else {
        val moai: Mu[Option[Array[Int]]] = Mu(None)  // TODO--can we use this?  Or should we drop it?
        val moadl: Mu[Option[Array[(Double, Long)]]] = Mu(None)  // Any point using this or shall we just slurp it later?
        val ts = c.times(storeStimuli = Some(moadl), storeTransitions = Some(moai)).yesOr(no => throw new IOException(no))
        val sm = new Summary()
        ts.foreach(t => sm add Summary.Entry(t))
        val seen = collection.mutable.Set.empty[Int]
        val mod = FileTime from Instant.now

        var currentBlock = 0
        var currentFill = 0
        def currentBlobsName = f"${c.baseString}_${currentBlock}%05dk.blobs"
        var currentText = Vector.newBuilder[String]
        def writeBlobsAndAdvance() {
          if (currentFill > 0) {
            txtcopy(currentBlobsName, mod).apply(Stored.Text(currentText.result))
            currentBlock += 1
            currentFill = 0
            currentText = Vector.newBuilder[String]
          }
        }
        def writeOneBlob(b: Blob, sm: Summary) {
          currentFill += 1
          currentText += s"% ${currentBlock*1000 + currentFill}"
          currentText ++= b.text(sm)
          if (currentFill == 1000) writeBlobsAndAdvance()
        }

        def writeSummary() {
          txtcopy(s"${c.baseString}.summary", mod).apply(Stored.Text(sm.text()))
        }

        c.visitAll(new MyVisitor {
          private def handleOneBlob(source: Vector[String], id: Int) {
            val b = Blob.from(id, source, keepSkeleton = false, keepOutline = true).yesOr(no => throw new IOException(no))
            ct.blob(id).apply(b) match {
              case Some(x) =>
                sm imprint x
                writeOneBlob(x, sm)
              case None => ()
            }
          }

          override def requestBlobs = true
          override def visitBlobData(name: String, modified: FileTime): FromStore.Text[Unit] = txt => {
            if (name.endsWith("blob")) {
              val id = c.getIdFromBlobName(name, seen).yesOr(no => throw new IOException(no))
              handleOneBlob(txt.lines, id)
            }
            else {
              var i = 0
              val seen = collection.mutable.HashSet.empty[Int]
              while (i < txt.lines.length && !txt.lines(i).startsWith("%")) i += 1
              while (i < txt.lines.length) {
                val id = c.getIdFromBlobsLine(txt.lines(i), name, i+1, seen).yesOr(no => throw new IOException(no))
                var j = i + 1
                while (j < txt.lines.length && !txt.lines(j).startsWith("%")) j += 1
                handleOneBlob(txt.lines.slice(i+1, j), id)
                i = j
              }
            }
          }
          override def noMoreBlobs() {
            someDataToWrite = currentBlock > 0 || currentFill > 0
            ct.stopBlobs().foreach{ blob => writeOneBlob(blob, sm) }
            writeBlobsAndAdvance()
          }

          override def requestSummary = someDataToWrite
          override def visitSummary(name: String, modified: FileTime): FromStore.Text[Unit] = txt => {
            val orig = Summary.from(txt.lines).yesOr(no => throw new IOException(no))
            sm adoptEvents orig
            writeSummary()
          }
          override def noSummary() {
            writeSummary()
          }
        }).?
      }
    }
    finally {
      if (zos ne null) zos.close
    }

    if (!someDataToWrite) { safe{ Files.delete(target) }.map(_ => None).mapNo(e => s"Could not delete empty $target\n${e.explain()}") }
    else {
      if (atomically) Files.move(target, finalTarget, StandardCopyOption.ATOMIC_MOVE)

      Contents.from[A](finalTarget, _ => Yes(base)).map(y => Some(y))
  }
  }
}
