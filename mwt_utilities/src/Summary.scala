package mwt.utilities

import java.io._
import java.nio.file._
import java.nio.file.attribute.FileTime
import java.time._
import java.util.zip._

import java.lang.{ StringBuilder => JStringBuilder }

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
  final def r1(x: Double): Double =
    if (x < 0) { if (x < -1e14) x else (x*10 - 0.5).toLong/1e1 }
    else       { if (x >  1e14) x else (x*10 + 0.5).toLong/1e1 }
  final def r2(x: Double): Double = 
    if (x < 0) { if (x < -1e13) x else (x*100 - 0.5).toLong/1e2 }
    else       { if (x >  1e13) x else (x*100 + 0.5).toLong/1e2 }
  final def r3(x: Double): Double = 
    if (x < 0) { if (x < -1e12) x else (x*1000 - 0.5).toLong/1e3 }
    else       { if (x >  1e12) x else (x*1000 + 0.5).toLong/1e3 }
  final def r4(x: Double): Double = 
    if (x < 0) { if (x < -1e11) x else (x*10000 - 0.5).toLong/1e4 }
    else       { if (x >  1e11) x else (x*10000 + 0.5).toLong/1e4 }

  class Entry private (val t: Double, val jtx: Double = 0.0, val jty: Double = 0.0, val jnx: Int = 0, val jny: Int = 0) {
    var n = 0
    var goodN = 0
    var persist = 0.0
    var speed = 0.0
    var angSpeed = 0.0
    var length = 0.0
    var relLength = 0.0
    var width = 0.0
    var relWidth = 0.0
    var aspect = 0.0
    var relAspect = 0.0
    var wiggle = 0.0
    var pixels = 0.0
    var stimString = ""
    var findLoss: Array[Int] = null
    var findLossN = 0
    def +=(that: Entry): this.type = {
      n += that.n
      if (that.goodN > 0) {
        if (goodN == 0) {
          persist   = that.persist
          speed     = that.speed
          angSpeed  = that.angSpeed
          length    = that.length
          relLength = that.relLength
          aspect    = that.aspect
          relAspect = that.relAspect
          wiggle    = that.wiggle
          pixels    = that.pixels
          goodN = that.goodN
        }
        else {
          val good = (goodN + that.goodN).toDouble
          persist   = (persist*goodN   + that.persist*that.goodN  ) / good
          speed     = (speed*goodN     + that.speed*that.goodN    ) / good
          angSpeed  = (angSpeed*goodN  + that.angSpeed*that.goodN ) / good
          length    = (length*goodN    + that.length*that.goodN   ) / good
          relLength = (relLength*goodN + that.relLength*that.goodN) / good
          width     = (width*goodN     + that.width*that.goodN    ) / good
          relWidth  = (relWidth*goodN  + that.relWidth*that.goodN ) / good
          aspect    = (aspect*goodN    + that.aspect*that.goodN   ) / good
          relAspect = (relAspect*goodN + that.relAspect*that.goodN) / good
          wiggle    = (wiggle*goodN    + that.wiggle*that.goodN   ) / good
          pixels    = (pixels*goodN    + that.pixels*that.goodN   ) / good
          goodN += that.goodN
        }
      }
      if (!that.stimString.isEmpty) {
        stimString =
          if (stimString.isEmpty) that.stimString
          else Entry.bitsToStimString(Entry.stimStringToBits(stimString) | Entry.stimStringToBits(that.stimString))        
      }
      if (that.findLossN > 0) {
        if (findLoss eq null) findLoss = java.util.Arrays.copyOf(that.findLoss, that.findLossN)
        else if (findLossN + that.findLossN <= findLoss.length) System.arraycopy(findLoss, findLossN, that.findLoss, 0, that.findLossN)
        else {
          findLoss = java.util.Arrays.copyOf(findLoss, math.max(findLossN + findLossN>>1 + 2, findLossN + that.findLossN))
          System.arraycopy(findLoss, findLossN, that.findLoss, 0, that.findLossN)
        }
        findLossN += that.findLossN
      }
      this
    }
    private[this] def addStimString(sb: JStringBuilder, prefix: String): JStringBuilder = 
      if (stimString.isEmpty) sb
      else sb append prefix append stimString
    private[this] def addFindLossString(sb: JStringBuilder, prefix: String): JStringBuilder = 
      if (findLossN < 2) sb
      else {
        sb append prefix
        var i = 0
        while (i+3 < findLossN) {
          sb append findLoss(i) append ' ' append findLoss(i+1) append ' '
          i += 2
        }
        sb append findLoss(i) append ' ' append findLoss(i+1)
      }
    private[this] def addPositionString(sb: JStringBuilder, prefix: String): JStringBuilder =
      if (jtx == 0 && jty == 0) sb
      else sb append prefix append r3(jtx) append ' ' append r3(jty) append "  " append jnx append ' ' append jny
    def addString(sb: JStringBuilder): JStringBuilder = {
      sb.ensureCapacity(100)
      sb append r3(t)
      sb append "  " append n append ' ' append goodN append ' ' append r1(persist)
      sb append "  " append r2(speed) append ' ' append r3(angSpeed)
      sb append "  " append r1(length) append ' ' append r3(relLength)
      sb append "  " append r1(width) append ' ' append r3(relWidth)
      sb append "  " append r3(aspect) append ' ' append r3(relAspect)
      sb append "  " append r3(wiggle) append ' ' append r3(pixels)
      addStimString(sb, "  % ")
      addFindLossString(sb, "  %% ")
      addPositionString(sb, "  @ ")
      sb
    }
    override def toString = addString(new JStringBuilder).toString
    override def equals(that: Any) = that match {
      case e: Entry =>
        t == e.t &&
        n == e.n && goodN == e.goodN && persist == e.persist &&
        speed == e.speed && angSpeed == e.angSpeed &&
        length == e.length && relLength == e.relLength &&
        width == e.width && relWidth == e.relWidth &&
        aspect == e.aspect && relAspect == e.relAspect &&
        wiggle == e.wiggle && pixels == e.pixels &&
        (
          stimString.isEmpty == e.stimString.isEmpty &&
          (if (stimString.nonEmpty) Entry.stimStringToBits(stimString) == Entry.stimStringToBits(e.stimString) else true)
        ) &&
        (
          findLossN == e.findLossN &&
          { var i = 0; while (i < findLossN && findLoss(i) == e.findLoss(i)) {}; i == findLossN }
        ) &&
        jtx == e.jtx && jty == e.jty && jnx == e.jnx && jny == e.jny
      case _ => false
    }
  }
  object Entry {
    def apply(t: Double): Entry = 
      if (t.finite) new Entry(t)
      else throw new IllegalArgumentException(s"Only finite times allowed in summary, not $t")
    def apply(t: Double, jtx: Double, jty: Double, jnx: Int, jny: Int): Entry =
      if (t.finite && jtx.finite && jty.finite) new Entry(t, jtx, jty, jnx, jny)
      else throw new IllegalArgumentException(s"Summary must have finite parameters; found t=$t, jtx=$jtx, jty=$jty")
    def parse(g: Grok)(implicit fail: GrokHop[g.type]): Entry = {
      g.delimit(true, 0)
      val index = g.I 
      val t = g.D
      val n = g.I
      val goodN = g.I
      val persist = g.D
      val speed = g.D
      val angSpeed = g.D
      val length = g.D
      val relLength = g.D
      val width = g.D
      val relWidth = g.D
      val aspect = g.D
      val relAspect = g.D
      val wiggle = g.D
      val pixels = g.D
      var x = g.peekTok
      val stimString =
        if ((x ne null) && x == "%") {
          g.skip
          val sb = new JStringBuilder
          while ({x = g.peekTok; (x ne null) && !x.startsWith("%") && !x.startsWith("@")}) {
            g.skip
            if (sb.length > 0) sb append ' '
            sb append x
          }
          sb.toString
        }
        else ""
      val findLoss =
        if ((x ne null) && x == "%%") {
          g.skip
          val ab = Array.newBuilder[Int]
          while ({x = g.peekTok; (x ne null) && !x.startsWith("%") && !x.startsWith("@")}) {
            ab += g.I
            ab += g.I
          }
          ab.result
        }
        else null
      while (g.hasContent && g.tok != "@") {}
      val entry =
        if (g.hasContent) apply(t, g.D, g.D, g.I, g.I)
        else apply(t)
      entry.n = n
      entry.goodN = goodN
      entry.persist = persist
      entry.speed = speed
      entry.angSpeed = angSpeed
      entry.length = length
      entry.relLength = relLength
      entry.width = width
      entry.relWidth = relWidth
      entry.aspect = relAspect
      entry.wiggle = wiggle
      entry.pixels = pixels
      if (stimString.nonEmpty) entry.stimString = stimString
      if (findLoss ne null) {
        entry.findLoss = findLoss
        entry.findLossN = findLoss.length
      }
      entry
    }
    def stimStringToBits(s: String): Long =
      if (s.startsWith("0x")) java.lang.Long.parseLong(s.substring(2), 16)
      else s.split("\\s+").map(x => 1L << (x.toInt-1).toLong).reduce(_ | _)
    def bitsToStimString(l: Long): String = "0x" + l.toHexString
  }
}
