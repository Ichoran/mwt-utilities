package mwt.utilities

import java.lang.{ StringBuilder => JStringBuilder }

import kse.flow._
import kse.maths._
import kse.coll.packed._
import kse.eio._

class Blob(val id: Int) extends TimedList[Blob.Entry] {
  protected def myDefaultSize = 16
}
object Blob extends TimedListCompanion {
  import Approximation._

  type MyElement = Entry
  type MyTimed = Blob

  class Entry(
    val t: Double, 
    val cx: Double, val cy: Double, val area: Int,
    val bx: Float, val by: Float, val len: Float, val wid: Float,
    val nSkel: Short, val nOut: Short, val ox: Short, val oy: Short, val data: Array[Int]
  )
  extends TimedElement {
    override def equals(that: Any): Boolean = that match {
      case e: Entry =>
        c4(t, e.t) && c4(cx, e.cx) && c4(cy, e.cy) && area == e.area &&
        c3(bx, e.bx) && c3(by, e.by) && c2(len, e.len) && c2(wid, e.wid) &&
        nSkel == e.nSkel && nOut == e.nOut && ox == e.ox && oy == e.oy && data.length == e.data.length &&
        {
          var i = 0
          while (i < data.length && data(i) == e.data(i)) i +=1
          i == data.length
        }
      case _ => false
    }
    def addSkeletonString(sb: JStringBuilder, prefix: String): JStringBuilder = {
      if (nSkel > 0) {
        sb append prefix
        var ss = data(0).asShorts
        sb append ss.s0 append ' ' append ss.s1
        var i = 1
        while (i < nSkel) {
          ss = data(i).asShorts
          sb append ' ' append ss.s0 append ' ' append ss.s1
          i += 1
        }
      }
      sb
    }
    def addOutlineString(sb: JStringBuilder, prefix: String): JStringBuilder = {
      if (nOut > 0) {
        sb append prefix append ox append ' ' append oy append ' ' append nOut append ' '
        var bits = 0
        var nBits = 0
        var i = nSkel.toInt
        val iN = i + (nOut >> 4)
        while (i < iN) {
          val x = data(i)
          bits = bits | ((x << nBits) & 0x3F)
          sb append (bits + '0').toChar
          sb append (((x >>> ( 6 - nBits)) & 0x3F) + '0').toChar
          sb append (((x >>> (12 - nBits)) & 0x3F) + '0').toChar
          sb append (((x >>> (18 - nBits)) & 0x3F) + '0').toChar
          sb append (((x >>> (24 - nBits)) & 0x3F) + '0').toChar
          bits = x >>> (30 - nBits)
          if (nBits == 4) {
            sb append (bits + '0').toChar
            nBits = 0
            bits = 0
          }
          else nBits += 2
          i += 1
        }
        if ((nOut & 0xF) != 0) {
          var bitz = data(iN).toLong
          var nBitz = 2*(nOut&0xF)
          if (nBits > 0) {
            val m = (1 << nBits) - 1
            bitz = (bitz << nBitz) | (bits & m)
            nBitz += nBits
          }
          var mask = (1L << nBitz)-1
          while (mask != 0) {
            sb append (((bitz & mask) & 0x3F) + '0').toChar
            bitz = bitz >>> 6
            mask = mask >>> 6
          }
        }
      }
      else if (nOut < 0) {
        sb append prefix
        var i = nSkel.toInt
        val iN = i - nOut
        while (i < iN) {
          val ss = data(i).asShorts
          sb append ' ' append ss.s0 append ' ' append ss.s1
          i += 1
        }
      }
      sb
    }
    def addString(sb: JStringBuilder): JStringBuilder = {
      sb append r3(t) append "  " append r3(cx) append ' ' append r3(cy) append "  " append area append "  "
      sb append r3(bx) append ' ' append r3(by) append "  0  " append r2(len) append ' ' append r2(wid)
      addSkeletonString(sb, " % ")
      addOutlineString(sb, " %% ")
    }
    override def toString = addString(new JStringBuilder).toString
  }
  object Entry {
    private val noSkeleton = new Array[Int](0)
    def parse(g: Grok, keepSkeleton: Boolean = true)(implicit fail: GrokHop[g.type]): Entry = {
      g.delimit(true, 0)
      g.skip
      val t = g.D
      val cx = g.D
      val cy = g.D
      val a = g.I
      val bx = g.F
      val by = g.F
      g.skip
      val len = g.F
      val wid = g.F
      var tok = g.peekTok
      var n = 0
      var buf = new Array[Int](11)
      if ((tok ne null) && tok == "%")  {
        g.skip
        tok = g.peekTok
        while ((tok ne null) && !tok.startsWith("%")) {
          val elt = g.S <> g.S
          if (buf.length <= n) buf = java.util.Arrays.copyOf(buf, buf.length*2)
          buf(n) = elt.I
          n += 1
          tok = g.peekTok
        }
        if (!keepSkeleton) n = 0
      }
      val nSkel = n
      var packed = false
      var ox: Short = 0
      var oy: Short = 0
      var opN = 0
      if ((tok ne null) && tok == "%%") {
        g.skip
        val x = g.I
        val y = g.I
        opN = (if (g.hasContent) g.I else 0)
        tok  = g.peekTok
        packed = (tok ne null) && ((opN > 0 && (tok.length*3 - opN).abs < 3) || x.abs > Short.MaxValue || y.abs > Short.MaxValue)
        if (!packed && (tok ne null)) {
          var i = 0
          while (!packed && i < tok.length) packed = !tok.charAt(i).isDigit
        }
        if (!packed) {
          if (buf.length <= n + (if (tok eq null) 0 else 1))
            buf = java.util.Arrays.copyOf(buf, if (tok eq null) buf.length + 1 else buf.length*2)
          buf(n) = (x.toShort <> y.toShort).I
          n += 1
          if (tok ne null) {
            buf(n) = (opN.toShort <> g.S).I
            n += 1
          }
          tok = g.peekTok
          while ((tok ne null) && !tok.startsWith("%")) {
            val elt = g.S <> g.S
            if (buf.length <= n) buf = java.util.Arrays.copyOf(buf, buf.length + (buf.length >> 1))
            buf(n) = elt.I
            n += 1
            tok = g.peekTok
          }
        }
        else {
          ox = x.toShort
          oy = y.toShort
          var bits = 0
          var bitN = 0
          var i = 0
          var j = opN
          while (i < tok.length) {
            val c = (tok.charAt(i) - '0') & 0x3F
            if (j < 3) {
              val cc = c & (0x3F >> (3 - j))
              bits = bits | (cc << bitN)
              bitN += 2*j
              j = 0
            }
            else {
              bits = bits | (c << bitN)
              bitN += 6
              j -= 3
            }
            if (bitN >= 32) {
              if (buf.length <= n) buf = java.util.Arrays.copyOf(buf, buf.length + (buf.length >> 1))
              buf(n) = bits
              n += 1
              bitN -= 32
              bits = if (bitN > 0) c >>> (6 - bitN) else 0
            }
            i += 1
          }
          if (bitN > 0) {
            if (buf.length <= n) buf = java.util.Arrays.copyOf(buf, n+1)
            buf(n) = bits
            n += 1
          }
          opN = opN - j
        } 
      }
      if (n == 0) buf = noSkeleton
      else if (n + (n >> 2) < buf.length) buf = java.util.Arrays.copyOf(buf, n)
      new Entry(t, cx, cy, a, bx, by, len, wid, nSkel.toShort, opN.toShort, ox, oy, buf)
    }
  }

  def from(id: Int, lines: Vector[String], keepSkeleton: Boolean = true): Ok[String, Blob] = myFrom(lines)(new Grokker {
    def title = "blob"
    def zero() = new Blob(id)
    def grok(g: Grok)(implicit gh: GrokHop[g.type]) = Entry.parse(g, keepSkeleton)
  })
}
