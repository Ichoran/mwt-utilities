package mwt.utilities

import java.io._
import java.nio.file._
import java.nio.file.attribute.FileTime
import java.nio.charset.StandardCharsets.UTF_8
import java.time._
import java.util.zip._

import scala.collection.JavaConverters._

import kse.flow._
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
    * is `Modify`. It passes back a handler that on the left branch creates a new block of blob text
    * given the old one, or requests for the blob to be dropped (`Right(false)`) or copied unchanged
    * (`Right(true)`).
    */
  def blob(n: Int): FromStore.Text[Either[Stored.Text, Boolean]] = FromStore.Text(x => Right(true))

  /** This is called when all blobs are done, but only if `modifyBlobs` is true.
    * The return is any new blobs that are to be created in addition to the old ones
    * (these blobs will get numbered automatically).
    */
  def stopBlobs(): Iterator[Stored.Text] = Iterator.empty

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
  def stopImages(): Iterator[(String, Stored.Binary)] = Iterator.empty

  /** This is called to handle each category of other kinds of file.  If `None` is given, then the files
    * are all skipped.  Otherwise, each filename is passed into the function to return an appropriate handler
    * that will pass along, regenerate, or delete (if `None` is returned) the data in that file.
    */
  def other(category: String): Option[String => FromStore[Option[Stored]]] = Some(_ => FromStore.Binary(x => Some(x)))

  /** This is called when all data has been seen.  The return specifies which additional files
    * to create, by "other" category number (i.e. file extension).  The returned Iterator is to
    * run through each file of that type.
    */
  def stop(): Map[String, Iterator[(String, Stored)]] = Map.empty
}
object ContentsTransformer {
  /** Copies existing files without changes */
  def default[A]: ContentsTransformer[A] = new ContentsTransformer[A] {}

  /** Cleans up existing files without losing much */
  def clean[A](minMove: Double, minTime: Double, target: Path): ContentsTransformer[A] = new ContentsTransformer[A] {
    private var newID = 0
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
      ((maxX - minX).sq + (maxY - minY).sq)/(if (maxL < 1) 1 else maxL).sq >= minMove.sq
    }
    override def relocate(here: Path) = target
    override def modifyBlobs() = true
    override def blob(n: Int) = FromStore.Text{ txt =>
      Blob.from(n, txt.lines, keepSkeleton = false) match {
        case No(_) => Right(false)  // Silently drop problems (?!)
        case Yes(b) =>
          if (b.length == 0) Right(false)
          else if (b(b.length-1).t - b.length < minTime) Right(false)
          else if (!movedEnough(b)) Right(false)
          else {
            newID += 1
            Left(Stored.Text((new Blob(newID)).loot(b).text()))
          }
      }
    }
  }
}


object CopyTransform {
  private def safeParent(path: Path): Path = {
    path.getParent                ReturnIf (_ ne null)
    path.toAbsolutePath.getParent ReturnIf (_ ne null)
    path.getFileSystem.getPath("")
  }

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

      abstract class MyVisitor extends FilesVisitor {
        override def requestImages = true
        override def visitImage(name: String, modified: FileTime) = ct.image(name).
          branch(_.toOk.swap.toEither, bincopy(name, modified))
        override def requestOthers(category: String) = true
        private[this] val myCategoryCache = collection.mutable.HashMap.empty[String, Option[String => FromStore[Option[Stored]]]]
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
      }

      
      if (!mod && c.summary.isDefined) c.visitAll(new MyVisitor {
        override def requestBlobs = true
        override def visitBlobData(name: String, modified: FileTime) = bincopy(name, modified)
        override def requestSummary = true
        override def visitSummary(name: String, modified: FileTime) = ct.summary() match {
          case Some(xf) => xf andThen txtcopy(name, modified)
          case _        => bincopy(name, modified)
        }
      })
      else {
        ???
      }
    }
    finally {
      if (zos ne null) zos.close
    }

    if (atomically) Files.move(target, finalTarget, StandardCopyOption.ATOMIC_MOVE)

    Contents.from[A](finalTarget, _ => Yes(base)).map(y => Some(y))
  }
}
