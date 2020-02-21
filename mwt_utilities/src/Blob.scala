package mwt.utilities

import java.lang.{ StringBuilder => JStringBuilder }

import kse.flow._
import kse.maths._
import kse.coll.packed._
import kse.eio._

class Blob(val id: Int) extends TimedList[Blob.Entry] {
  protected def myDefaultSize = 16

  def speeds(window: Double): (Array[Float], Array[Float]) = {
    val sa = new Array[Float](length)
    val wa = new Array[Float](length)
    if (length < 2) return (sa, wa)
    var i0 = 0
    var i = 0
    var i1 = 0
    var t0 = apply(i).t
    var t  = t0
    var t1 = t0

    // Slide along to find appropriate endpoints for time interval
    while (i < length) {
      t = apply(i).t
      while (t - t0 < 0.49*window && i0 > 0) { i0 -= 1; t0 = apply(i0).t }
      while (t1 - t < 0.49*window && i1+1 < length) { i1 += 1; t1 = apply(i1).t }
      var shrinkable = (t1 - t0) > window && (i0+1 < i || i+1 < i1)
      while (shrinkable) {
        val t0a = if (i0+1 < i) apply(i0+1).t else t0
        val t1a = if (i < i1-1) apply(i1-1).t else t1
        if (t1a - t <= 0.95*window || i+1 >= i1) {
          if (t - t0a <= 0.95*window || i0+1 >= i) shrinkable = false
          else {
            t0 = t0a
            i0 += 1
          }
        }
        else if (t - t0a <= 0.95*window || i0+1 >= i) {
          if (t1a - t <= 0.95*window || i+1 >= i1) shrinkable = false
          else {
            t1 = t1a
            i1 -= 1
          }
        }
        else {
          if (t1 - t > t - t0) {
            t1 = t1a
            i1 -= 1
          }
          else {
            t0 = t0a
            i0 += 1
          }
        }
      }

      // Measure speeds with this time interval (rough estimate)
      val idt = 1.0/(t1-t0 max 0.5*window)

      // Speed calculation (with ad-hoc partial compensation for reversal)
      val v0 = Vc.from(apply(i0).cx, apply(i0).cy)
      val v  = Vc.from(apply(i).cx,  apply(i).cy)
      val v1 = Vc.from(apply(i1).cx, apply(i1).cy)
      val d2  = v1 distSq v0
      val d2a = v  distSq v0
      val d2b = v1 distSq v
      sa(i) = ((if (2*d2 >= d2a + d2b) d2 else 0.5*(d2a+d2b)).sqrt * idt).toFloat

      // Angular velocity calculation (with ad-hoc partial compensation for reverse-swing)
      val a0 = Vc.from(apply(i0).bx, apply(i0).by)
      val a  = Vc.from(apply(i).bx,  apply(i).by)
      val a1 = Vc.from(apply(i1).bx, apply(i1).by)
      val dot  = (a1 * a0).abs
      val dota = (a  * a0).abs
      val dotb = (a1 * a ).abs
      wa(i) = ((if (2*(1 - dot) >= 2 - dota - dotb) dot else 0.5*(dota+dotb)).clip(0, 1).acos * idt).toFloat

      i += 1
    }
    (sa, wa)
  }

  def text(that: TimedList[_]): Vector[String] = {
    var last = 0
    text((_, e) => { last = (last + 1) max (that.indexOf(e.t) + 1); last.toString })
  }

  def text(): Vector[String] = text((_, e) => e.frame.toString)

  override def toString = (s"% $id" +: text((i, e) => if (i < 6) e.frame.toString else null)).mkString("\n")
}
object Blob extends TimedListCompanion {
  import Approximation._

  type MyElement = Entry
  type MyTimed = Blob

  case class Mask(x0: Int, x1: Int, y0: Int, y1: Int, pixels: Array[Byte]) {}
  object Mask {
    def isMaskable(xs: Array[Short], ys: Array[Short]): Boolean =
      if (xs.length < 1 || ys.length < 1) false
      else if (xs.length != ys.length) false
      else (xs(0) - xs(xs.length-1)).abs + (ys(0) - ys(ys.length-1)).abs < 2
    def isMaskable(outline: (Array[Short], Array[Short])): Boolean = isMaskable(outline._1, outline._2)

    def maskableOutlineFrom(xs: Array[Short], ys: Array[Short]): Option[(Array[Short], Array[Short])] = {
      if (xs.length < 1 || ys.length < 1) return None
      val len = xs.length min ys.length
      val gap = (xs(0) - xs(len-1)).abs + (ys(0) - ys(len-1)).abs
      if (xs.length == ys.length && gap < 2) return Some((xs, ys))
      val nxs = java.util.Arrays.copyOf(xs, xs.length + gap - 1)
      val nys = java.util.Arrays.copyOf(ys, ys.length + gap - 1)
      val x0 = xs(0)
      val y0 = ys(0)
      var i = xs.length
      while (i < nxs.length) {
        val xi = nxs(i-1)
        val yi = nys(i-1)
        val gapx = x0 - xi
        val gapy = y0 - yi
        val agapx = gapx.abs
        val agapy = gapy.abs
        val xk = xi + (if (gapx > 0) 1 else -1)
        val yk = yi + (if (gapy > 0) 1 else -1)
        if (agapx < agapy) { 
          nxs(i) = xi
          nys(i) = yk.toShort
        }
        else if (agapx > agapy) {
          nxs(i) = xk.toShort
          nys(i) = yi
        }
        else {
          val xsq = (xi - xk).sq
          val ysq = (yi - yk).sq
          var decision = 0
          var j = 1
          while (decision == 0 && j < xs.length) {
            val xj = xs(j)
            val yj = ys(j)
            val dsqA = xsq + (yj - yk).sq
            val dsqB = (xj - xk).sq + ysq
            if (dsqA > dsqB) decision = 1
            else if (dsqA < dsqB) decision = -1
            j += 1
          }
          if (decision > 0) {
            nxs(i) = xi
            nxs(j) = yk.toShort
          }
          else {
            nxs(i) = xk.toShort
            nys(i) = yi
          }
        }
        i += 1
      }
      Some((nxs, nys))
    }
    def maskableOutlineFrom(outline: (Array[Short], Array[Short])): Option[(Array[Short], Array[Short])] =
      if (isMaskable(outline._1, outline._2)) Some(outline)
      else maskableOutlineFrom(outline._1, outline._2)

    private def floodZero(w: Int, pixels: Array[Byte], value: Byte) {
      var work = new Array[Long](8)
      work(0) = Shorts(0, 1, -1, 1).L
      val h = pixels.length / w
      var n = 1
      while (n > 0) {
        val wk = work(n-1).asShorts
        n -= 1
        val x0 = wk.s0.toInt
        val xN = wk.s1.toInt
        val y  = wk.s2 + wk.s3
        var x  = x0.toInt
        if (y >= 0 && y < h) {
          while (pixels(x + w*y) != 0 && x < xN) x += 1
          if (x < xN) {
            var xx = x+1
            if (x == x0) while (x > 0 && pixels(x-1 + w*y) == 0) x -= 1
            while (xx < w && pixels(xx + w*y) == 0) xx += 1
            val m = n + 1 + (if (x < x0-1) 1 else 0) + (if (xx > xN+1) 1 else 0)
            if (work.length < m) work = java.util.Arrays.copyOf(work, 2*work.length)
            if (x < x0-1) {
              work(n) = Shorts(x.toShort, (x0-1).toShort, y.toShort, (-wk.s3).toShort).L
              n += 1
            }
            if (xx > xN+1) {
              work(n) = Shorts((xN+1).toShort, xx.toShort, y.toShort, (-wk.s3).toShort).L
              n += 1
            }
            work(n) = Shorts(x.toShort, xx.toShort, y.toShort, wk.s3).L
            n += 1
            while (x < xx) { pixels(x + w*y) = value; x += 1 }
          }
        }
      }
    }

    def fromOutline(outline: (Array[Short], Array[Short]), inverted: Boolean = false, transposed: Boolean = false): Option[Mask] = {
      val (xs, ys) = {
        val xy = maskableOutlineFrom(outline).?
        if (transposed) (xy._2, xy._1)
        else xy
      }
      val n = xs.length min ys.length
      var x0, x1 = xs(0).toInt
      var y0, y1 = ys(0).toInt
      var i = 1
      while (i < n) {
        val xi = xs(i)
        val yi = ys(i)
        if (xi < x0) x0 = xi
        else if (xi > x1) x1 = xi
        if (yi < y0) y0 = yi
        else if (yi > y1) y1 = yi
        i += 1
      }
      x0 -= 1
      x1 += 1
      y0 -= 1
      y1 += 1
      val w = 1 + x1 - x0
      val h = 1 + y1 - y0
      val pixels = new Array[Byte](w*h)
      i = 0
      while (i < n) {
        pixels(xs(i) - x0 + w*(ys(i) - y0)) = 1
        i += 1
      }
      floodZero(w, pixels, 255.toByte)
      if (inverted) {
        i = 0
        while (i < n) {
          pixels(xs(i) - x0 + w*(ys(i) - y0)) = 0
          i += 1
        }        
      }
      else {
        i = 0
        while (i < pixels.length) {
          val pi = pixels(i)
          pixels(i) = if (pi == 255.toByte) 0 else 255.toByte
          i += 1
        }
      }
      Some(new Mask(x0, x1, y0, y1, pixels))
    }
  }

  class Entry(
    var frame: Int,
    val t: Double, 
    val cx: Double, val cy: Double, val area: Int,
    val bx: Float, val by: Float, val len: Float, val wid: Float,
    val nSkel: Short, val nOut: Short, val ox: Short, val oy: Short, val data: Array[Int]
  )
  extends TimedElement {
    private[this] var myWiggle = Float.NaN

    def asp = if (len > 0) wid/len else 0

    def wig = {
      if (myWiggle.isNaN) {
        myWiggle =
          if (nSkel == 0) 0.0f
          else {
            val na = 1+nSkel/5
            val nb = nSkel - na - 1
            if (na >= nb) 0.0f
            else {
              val c0 = data(0      ).asShorts
              val ca = data(na     ).asShorts
              val cz = data(na+1   ).asShorts
              val cy = data(nb-1   ).asShorts
              val cb = data(nb     ).asShorts
              val c1 = data(nSkel-1).asShorts
              val vha = Vc(c0.s0 - ca.s0, c0.s1 - ca.s1)
              val vta = Vc(cz.s0 - c1.s0, cz.s1 - c1.s1)
              val dota = vha dotHat vta
              val vhb = Vc(c1.s0 - cb.s0, c1.s1 - cb.s1)
              val vtb = Vc(cy.s0 - c0.s0, cy.s1 - c0.s1)
              val dotb = vhb dotHat vtb
              (dota min dotb).clip(0, 1).acos.toFloat
            }
          }
      }
      myWiggle
    }

    def skeleton: Option[(Array[Short], Array[Short])] =
      if (nSkel <= 0) None
      else {
        val xs, ys = new Array[Short](nSkel)
        var i = 0
        while (i < xs.length) {
          val ss = data(i).asShorts
          xs(i) = ss.s0
          ys(i) = ss.s1
          i += 1
        }
        Some((xs, ys))
      }

    def outline(close: Boolean = false): Option[(Array[Short], Array[Short])] =
      if (nOut <= 0) None
      else {
        val xs, ys = new Array[Short](1 + nOut)
        var j = 0
        var x = ox.toInt
        var y = oy.toInt
        xs(j) = x.toShort
        ys(j) = y.toShort
        j += 1
        while (j < xs.length) {
          var bits = data(nSkel + (j >>> 4))
          var i = 16 min (nOut - (j-1))
          while (i > 0) {
            (bits & 0x3) match {
              case 0 => x -= 1
              case 1 => x += 1
              case 2 => y -= 1
              case _ => y += 1
            }
            xs(j) = x.toShort
            ys(j) = y.toShort
            j += 1
            bits = bits >>> 2
            i -= 1
          }
        }
        if (!close) Some((xs, ys))
        else Mask.maskableOutlineFrom(xs, ys)
      }

    def mask(inverted: Boolean = false, transposed: Boolean = false): Option[Mask] =
      if (nOut <= 0) None
      else outline(false).flatMap(o => Mask.fromOutline(o, inverted, transposed))

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
          bits = bits | (x << nBits)
          sb append (( bits         & 0x3F) + '0').toChar
          sb append (((bits >>>  6) & 0x3F) + '0').toChar
          sb append (((bits >>> 12) & 0x3F) + '0').toChar
          sb append (((bits >>> 18) & 0x3F) + '0').toChar
          sb append (((bits >>> 24) & 0x3F) + '0').toChar
          bits = (bits >>> 30) | (if (nBits > 0) x >>> (30 - nBits) else 0)
          nBits += 2
          if (nBits == 6) {
            sb append ((bits & 0x3F) + '0').toChar
            nBits = 0
            bits = 0
          }
          i += 1
        }
        if (nBits > 0 || (nOut & 0xF) != 0) {
          var nRemain = 2*(nOut & 0xF)
          var remains = (if (nRemain > 0) data(iN).toLong else 0L)
          if (nBits > 0) {
            val m = (1 << nBits) - 1
            remains = (remains << nBits) | (bits & m)
            nRemain += nBits
          }
          var mask = (1L << nRemain)-1
          while (mask != 0) {
            sb append (((remains & mask) & 0x3F) + '0').toChar
            remains = remains >>> 6
            mask    = mask    >>> 6
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
    def parse(g: Grok, keepSkeleton: Boolean = true, keepOutline: Boolean = true)(implicit fail: GrokHop[g.type]): Entry = {
      g.delimit(true, 0)
      val frame = g.I
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
      if ((tok ne null) && tok == "%%" && keepOutline) {
        g.skip
        val x = g.I
        val y = g.I
        opN = (if (g.hasContent) g.I else 0)
        tok  = g.peekTok
        packed = (tok ne null) && ((opN > 0 && (tok.length*3 - opN).abs < 3) || x.abs > Short.MaxValue || y.abs > Short.MaxValue)
        if (!packed && (tok ne null)) {
          var i = 0
          while (!packed && i < tok.length) { packed = !tok.charAt(i).isDigit; i += 1 }
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
          opN = nSkel - n  // Use negative number to indicate unpacked
        }
        else {
          ox = x.toShort
          oy = y.toShort
          var bits = 0
          var bitN = 0
          var i = 0
          var j = opN
          while (i < tok.length && j > 0) {
            val inc = math.min(3, j)*2
            val c = (tok.charAt(i) - '0') & (0x3F >> (6-inc))
            bits = bits | (c << bitN)
            bitN += inc
            if (bitN >= 32) {
              if (buf.length <= n) buf = java.util.Arrays.copyOf(buf, buf.length + (buf.length >> 1))
              buf(n) = bits
              n += 1
              bitN -= 32
              bits = if (bitN > 0) c >>> (inc - bitN) else 0
            }
            j -= inc/2
            i += 1
          }
          if (bitN > 0) {
            if (buf.length <= n) buf = java.util.Arrays.copyOf(buf, n+1)
            buf(n) = bits
            n += 1
          }
        } 
      }
      if (n == 0) buf = noSkeleton
      else if (n + (n >> 2) < buf.length) buf = java.util.Arrays.copyOf(buf, n)
      new Entry(frame, t, cx, cy, a, bx, by, len, wid, nSkel.toShort, opN.toShort, ox, oy, buf)
    }
  }

  private def myGrokker(id: Int, keepSkeleton: Boolean, keepOutline: Boolean): Grokker = new Grokker {
    def title = "blob"
    def zero() = new Blob(id)
    def grok(g: Grok)(implicit gh: GrokHop[g.type]) = Entry.parse(g, keepSkeleton, keepOutline)
  }

  def from(id: Int, lines: Vector[String], keepSkeleton: Boolean = true, keepOutline: Boolean = true): Ok[String, Blob] = 
    myFrom(lines)(myGrokker(id, keepSkeleton, keepOutline))

  // TODO -- make a proper test of this
  object UnitTest {
    import kse.maths.stochastic.Pcg64
    def test_outline_bits(r: Pcg64 = new Pcg64): Ok[(Int, String, Option[Entry]), Unit] = {
      (1 to 66).iterator.map{ n => 
        Iterator.
          continually{ 
            val sb = new java.lang.StringBuilder
            var nb = 2*n
            while (nb > 0) { sb append ('0' + (r % (1 << math.min(6, nb)))).toChar; nb -= 6 }
            sb.toString
          }.
          map(x => x -> s"1 0.0  0.0 0.0  42  0.0 0.0  0  9.9 1.1 %% 0 0 $n $x").
          map{ case (x, s) => 
            val g = Grok(s);
            (n, x, s, g{ implicit fail => mwt.utilities.Blob.Entry.parse(g) })
          }.
          take(1000).
          find{ case (_, x, _, e) => 
            !e.isOk || !(e.yes.toString.split("\\s+").last == x)
          }
      }.find(_.isDefined).flatten match {
        case None            => Ok.UnitYes
        case Some((n, x, s, e)) => No((n, x, e.toOption))
      }
    }
  }
}
