package mwt.utilities

import java.lang.{ StringBuilder => JStringBuilder }

import kse.flow._
import kse.maths._
import kse.maths.packed._
import kse.eio._

class Blob() {

}
object Blob() {
  import Approximation._

  class Entry(val t: Double, val cx: Double, val cy: Double, val area: Int, val bx: Float, val by: Float, val len: Float, val wid: Float, val nSkel: Short, val nOut: Short, val ox: Short, val oy: Short, val data: Array[Int]) {
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
        sb append ss.s1 append ' ' append ss.s2
        var i = 1
        while (i < nSkel) {
          ss = data(i).asShorts
          sb append ' ' append ss.s1 append ' ' append ss.s2
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
        var i = nSkel
        val iN = nSkel + (nOut >> 4)
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
          var bitz = data(iN)
          var nBitz = 2*(nOut&0xF)
          ???
        }
      }
      else if (nOut < 0) {
        sb append prefix
        var i = nSkel
        val iN = nSkel - nOut
        while (i < iN) {
          val ss = data(i).asShorts
          sb append ' ' append ss.s1 append ' ' append ss.s2
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
}