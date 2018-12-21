package mwt.utilities

import java.io._
import java.nio.file._
import java.time._

import scala.collection.JavaConverters._

import kse.flow._
import kse.eio._


trait ImageAcceptor {
  def acceptable(name: String): Boolean
  def accept(bytes: Array[Byte]): Ok[String, Unit]
}

trait ImageDecoder[+A] { self =>
  def decodable(name: String): Boolean
  def decode(bytes: Array[Byte]): Ok[String, A]
  def asAcceptor(callback: A => Unit): ImageAcceptor = new ImageAcceptor {
    def acceptable(name: String): Boolean = self.decodable(name)
    def accept(bytes: Array[Byte]): Ok[String, Unit] = self.decode(bytes) match {
      case Yes(y) => callback(y); Ok.UnitYes
      case n: No[String] => n
    }
  }
}


abstract class ImageByExtension(val extension: String)
extends ImageDecoder[java.awt.image.BufferedImage] {
  def decodable(name: String) =
    if (extension.length > 0 && extension.charAt(0) != '.') name endsWith ("."+extension)
    else name endsWith extension
  def asEntry: (String, Option[ImageByExtension]) =
    (if (extension.length > 0 && extension.charAt(0) != '.') "." + extension else extension, Some(this))
}


object ImageDecoder {
  import java.awt.Transparency
  import java.awt.image._
  import java.awt.color._

  def graySpace = ColorSpace getInstance ColorSpace.CS_GRAY

  val DoNotDecode = new ImageAcceptor {
    def acceptable(name: String) = false
    def accept(bytes: Array[Byte]) =
      throw new IllegalArgumentException("Tried to decode something despite refusal to decode anything")
  }

  private class MwtRawDecoder() extends ImageByExtension(".raw") {
    def decode(bytes: Array[Byte]): Ok[String, BufferedImage] = {
      val buf = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).asShortBuffer
      if (buf.remaining < 2) return No(s"Input data too small to contain image size (${bytes.length} bytes)")
      val nx = buf.get()
      val ny = buf.get()
      if (nx < 0 || ny < 0 || nx.toLong*ny.toLong >= Int.MaxValue) return No(s"Input data not a sensible size: $nx x $ny")
      if (buf.remaining < nx*ny) return No(s"Insufficient elements for image ($nx x $ny claimed but have only ${buf.remaining})")
      val data = new Array[Short](nx*ny)
      buf.get(data)
      val dbs = new DataBufferShort(data, data.length)
      val ccm = new ComponentColorModel(graySpace, Array(8), false, true, Transparency.OPAQUE, DataBuffer.TYPE_SHORT)
      val wr = Raster.createWritableRaster(ccm.createCompatibleSampleModel(nx, ny), dbs, null)
      Yes(new BufferedImage(ccm, wr, false, null))
    }
  }
  val mwtRawDecoder: ImageByExtension = new MwtRawDecoder()

  private class MwtRaw8Decoder() extends ImageByExtension(".raw8") {
    def decode(bytes: Array[Byte]): Ok[String, BufferedImage] = {
      val buf = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN)
      if (buf.remaining < 4) return No(s"Input data too small to contain image size (${bytes.length} bytes)")
      val nx = buf.getShort()
      val ny = buf.getShort()
      if (nx < 0 || ny < 0 || nx.toLong*ny.toLong >= Int.MaxValue) return No(s"Input data not a sensible size: $nx x $ny")
      if (buf.remaining < nx*ny) return No(s"Insufficient elements for image ($nx x $ny claimed but have only ${buf.remaining})")
      val data = new Array[Byte](nx*ny)
      buf.get(data)
      val dbs = new DataBufferByte(data, data.length)
      val ccm = new ComponentColorModel(graySpace, false, true, Transparency.OPAQUE, DataBuffer.TYPE_BYTE)
      val wr = Raster.createWritableRaster(ccm.createCompatibleSampleModel(nx, ny), dbs, null)
      Yes(new BufferedImage(ccm, wr, false, null))
    }
  }
  val mwtRaw8Decoder: ImageByExtension = new MwtRaw8Decoder()


  trait ImageFromImageIO extends ImageDecoder[BufferedImage] {
    def decode(bytes: Array[Byte]): Ok[String, BufferedImage] = safe {
      val bais = new ByteArrayInputStream(bytes)
      javax.imageio.ImageIO.read(bais).
        tap(img => if (img == null) return No(s"No decoder available in ImageIO"))
    }.mapNo(e => s"Failure to decode with ImageIO:\n${e.explain(64)}")
  }
  private class PngDecoder() extends ImageByExtension(".png") with ImageFromImageIO {}
  val pngDecoder: ImageByExtension = new PngDecoder()
  private class TiffDecoder(extension: String) extends ImageByExtension(extension) with ImageFromImageIO {}
  val tiffDecoder: ImageByExtension = new TiffDecoder(".tiff")
  val tifDecoder: ImageByExtension = new TiffDecoder(".tif")

  val library: Map[String, Option[ImageByExtension]] = Map(
    mwtRawDecoder.asEntry,
    mwtRaw8Decoder.asEntry,
    tiffDecoder.asEntry,
    tifDecoder.asEntry,
    pngDecoder.asEntry,
    ".dbde" -> None
  )
}
