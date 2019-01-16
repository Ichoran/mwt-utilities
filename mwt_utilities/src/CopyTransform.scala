package mwt.utilities

import java.io._
import java.nio.file._
import java.nio.file.attribute.FileTime
import java.time._
import java.util.zip._

import scala.collection.JavaConverters._

import kse.flow._
import kse.eio._


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
    * be generated, or if the blobs were changed/renumbered (and therefore the summary file needed modification too)
    */
  def summary(): FromStore[Stored] = FromStore.All(x => x)

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
      if (prefix == c.target.prefix) name
      else {
        val bits = name.split('_')
        val stamp = (if (bits.length <= 2) name else name.takeRight(2).mkString("_"))
        val ans = prefix match {
          case None    => stamp
          case Some(p) => p + "_" + stamp
        }
        if (zip == c.target.isZip) ans
        else if (zip)              ans + ".zip"
        else                       ans.dropRight(4)
      }
    }
    var tempName = (if (atomically) whoName + ".atomic" else whoName)

    val target = path resolve tempName
    var finalTarget = path resolve whoName

    if (Files exists finalTarget) return No(s"Could not store ${c.who} because target exists:\n$finalTarget")
    if (atomically && Files.exists(target)) return No(s"Could not store ${c.who} because temporary location exists:\n$target")

    val mod = ct.modifyBlobs

    val zos = safe {
      if (zip) new ZipOutputStream(new FileOutputStream(target.toFile))
      else (Files createDirectories target) pipe (_ => null)
    } TossAs (s"Could not write $target\n" + _.explain())

    val zf = safe {
      if (c.target.isZip) new ZipFile(c.who.toFile)
      else null
    } TossAs { e => safe{zos.close}; s"Could not open ${c.who}\n" + e.explain() }

    try {
      var phase = 0
      while (phase < 5) { 
        phase += 1
        phase match {
          case 1 =>
            if (!mod) {
              val binaries: Iterator[Ok[String, (String, FileTime, Array[Byte])]] =
                if (c.target.isZip) new Iterator[Ok[String, (String, FileTime, Array[Byte])]] {
                  private val pending = collection.mutable.HashSet(c.blobs: _*)
                  private val zen = zf.entries.asScala
                  private var oze: Option[ZipEntry] = None
                  private var complete = false
                  def hasNext: Boolean =
                    if (complete) false
                    else if (oze.isDefined) true
                    else {
                      while (zen.hasNext && !oze.isDefined) {
                        val ze = zen.next
                        if (pending contains ze.getName) {
                          oze = Some(ze)
                          pending - ze.getName
                        }
                      }
                      if (oze.isEmpty && !zen.hasNext) complete = true
                      !complete
                    }
                  def next(): Ok[String, (String, FileTime, Array[Byte])] = {
                    if (!hasNext || !oze.isDefined) return No(s"Tried to get next blob file in ${c.who} but there was none")
                    val ze = oze.get
                    oze = None
                    val i = ze.getName.lastIndexOf('\\')
                    val j = ze.getName.lastIndexOf('/')
                    val k = (if (i < 0) j else if (j < 0) i else i max j)
                    val nm = if (k < 0) ze.getName else ze.getName.substring(k+1)
                    zf.getInputStream(ze).gulp.map(bytes => (nm, ze.getLastModifiedTime, bytes))
                  }
                }
                else c.blobs.iterator.map{ b => 
                  val p = c.who resolve b
                  val t = Files.getLastModifiedTime(p)
                  p.toFile.gulp.map(bytes => (b, t, bytes))
                }
              if (zos eq null) binaries.foreach{ x => x.? match { case (nm, t, b) =>
                val p = target resolve nm
                Files.write(p, b)
                Files.setLastModifiedTime(p, t)
              }}
              else binaries.foreach{ x => x.? match { case (nm, t, b) =>
                val ze = new ZipEntry(finalTarget.getFileName.toString + "/" + nm)
                ze.setLastModifiedTime(t)
                zos.putNextEntry(ze)
                zos.write(b, 0, b.length)
                zos.closeEntry()
              }}
            }
            else {

            }
          case 2 => 
            if (!mod && c.summary.isDefined) {

            }
            else {

            }
          case 3 => // Other phase
          case _ => // Extra files phase
        }
      }
    }
    finally { 
      if (zf ne null) zf.close
      if (zos ne null) zos.close
    }

    null
  }
}
