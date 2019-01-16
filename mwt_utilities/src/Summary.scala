package mwt.utilities

import java.io._
import java.nio.file._
import java.nio.file.attribute.FileTime
import java.time._
import java.util.zip._

import scala.collection.JavaConverters._

import kse.flow._
import kse.maths._
import kse.eio._


/** This class holds .summary file data (one of the old MWT data output files) */
class Summary() {
  private[this] var lines: Array[Summary.Entry] = new Array[Summary.Entry](256)
  private[this] var linesN = 0
  private[this] val extra = collection.mutable.TreeMap.empty[Double, Summary.Entry]

  def pack(): this.type = {
    if (extra.size > 0) {
      val ex = {
        val temp = new Array[Summary.Entry](extra.size)
        var i = 0
        extra.valuesIterator.foreach{ e => temp(i) = e; i += 1 }
        temp
      }
      extra.clear
      val m = linesN + ex.length
      if (m <= lines.length) {
        var i = linesN - 1
        var j = ex.length - 1
        var k = m - 1
        while (k > i) {
          lines(k) =
            if (i < 0)                      { val ans = ex(j);    j -= 1; ans }
            else if (ex(j).t >= lines(i).t) { val ans = ex(j);    j -= 1; ans }
            else                            { val ans = lines(i); i -= 1; ans }
          k -= 1
        }
      }
      else {
        val n = math.max(lines.length + lines.length >> 1, m + m >> 2)
        val old = lines
        lines = new Array[Summary.Entry](n)
        var i = 0
        var j = 0
        var k = 0
        while (k < m) {
          lines(k) =
            if (i >= linesN)                { val ans = old(i); i += 1; ans }
            else if (j >= ex.length)        { val ans = ex(j);  j += 1; ans }
            else if (ex(j).t >= lines(i).t) { val ans = old(i); i += 1; ans }
            else                            { val ans = ex(j);  j += 1; ans }
          k += 1
        }
      }
      linesN = m
    }
    this
  }

  private[this] def mySeek(t: Double): Summary.Entry = {
    if (linesN > 0) {
      var i = 0
      var j = linesN - 1
      while (i < j) {
        val k = (i + j) >> 1;
        val lkt = lines(k).t
        if (lkt == t)     return lines(k)
        else if (lkt < t) i = k+1
        else              j = k-1
      }
      if (i == j && lines(i).t == t) return lines(i)
    }
    if (extra.nonEmpty) extra.getOrElse(t, null)
    else null
  }

  private[this] def myAddMissing(e: Summary.Entry): Summary.Entry = {
    if (linesN < lines.length && (linesN == 0 || lines(linesN-1).t < e.t)) {
      lines(linesN) = e
      linesN += 1
    }
    else extra(e.t) = e
    e
  }

  def add(e: Summary.Entry): Summary.Entry = {
    val existing = mySeek(e.t)
    if (existing ne null) { existing += e; existing }
    else myAddMissing(e)
  }

  def get(t: Double):  Option[Summary.Entry] = Option(mySeek(t))

  def apply(t: Double): Summary.Entry = {
    mySeek(t) ReturnIf (_ ne null)
    Summary.Entry(t) tap myAddMissing
  }
}
object Summary {
  class Entry private (val t: Double) {
    def +=(that: Entry): this.type = this
  }
  object Entry {
    def apply(t: Double) = 
      if (t.finite) new Entry(t)
      else throw new IllegalArgumentException(s"Only finite times allowed in summary, not $t")
  }
}
