package mwt.utilities

import java.lang.{ StringBuilder => JStringBuilder }

import kse.flow._
import kse.maths._
import kse.eio._


/** This class holds .summary file data (one of the old MWT data output files) */
class Summary extends TimedMonoidList[Summary.Entry] {
  protected def myDefaultSize = 256
  protected def emptyElement(t: Double) = Summary.Entry(t)
  protected def mutableMerge(existing: Summary.Entry, novel: Summary.Entry) { existing += novel }

  def text: Vector[String] = {
    pack()
    if (myLinesN == 0) Vector.empty[String]
    else {
      val vb = Vector.newBuilder[String]
      vb.sizeHint(myLinesN)
      var i = 0; while (i < myLinesN) { vb += myLines(i).toString; i += 1 }
      vb.result
    }
  }
}
object Summary extends TimedListCompanion {
  import Approximation._

  type MyElement = Entry
  type MyTimed = Summary

  class Entry private (val t: Double, val jtx: Double = 0.0, val jty: Double = 0.0, val jnx: Int = 0, val jny: Int = 0)
  extends TimedElement {
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
    var stimuli = 0L
    var findLoss: Array[Int] = null
    var findLossN = 0
    def changeIdentity(prev: Int, succ: Int): this.type = {
      if (findLoss eq null) findLoss = new Array[Int](4)
      else if (findLossN + 2 > findLoss.length) findLoss = java.util.Arrays.copyOf(findLoss, findLoss.length + math.max(findLoss.length >> 1, 4))
      findLoss(findLossN) = prev
      findLoss(findLossN+1) = succ
      findLossN += 1
      this
    }
    def findIdentity(id: Int): this.type = changeIdentity(0, id)
    def loseIdentity(id: Int): this.type = changeIdentity(id, 0)
    def include(per: Double, sp: Double, aSp: Double, len: Double, rLen: Double, wid: Double, rWid: Double, asp: Double, rAsp: Double, wig: Double, pix: Int): this.type = {
      if (goodN == 0) {
        persist   = per
        speed     = sp
        angSpeed  = aSp
        length    = len
        relLength = rLen
        width     = wid
        relWidth  = rWid
        aspect    = asp
        relAspect = rAsp
        wiggle    = wig
        pixels    = pix
      }
      else {
        persist   = ((persist   * goodN) + per ) / (goodN + 1)
        speed     = ((speed     * goodN) + sp  ) / (goodN + 1)
        angSpeed  = ((angSpeed  * goodN) + aSp ) / (goodN + 1)
        length    = ((length    * goodN) + len ) / (goodN + 1)
        relLength = ((relLength * goodN) + rLen) / (goodN + 1)
        width     = ((width     * goodN) + wid ) / (goodN + 1)
        relWidth  = ((relWidth  * goodN) + rWid) / (goodN + 1)
        aspect    = ((aspect    * goodN) + asp ) / (goodN + 1)
        relAspect = ((relAspect * goodN) + rAsp) / (goodN + 1)
        wiggle    = ((wiggle    * goodN) + wig ) / (goodN + 1)
        pixels    = ((pixels    * goodN) + pix ) / (goodN + 1)
      }
      goodN += 1
      this
    }
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
      stimuli |= that.stimuli
      if (that.findLossN > 0) {
        if (findLoss eq null) findLoss = java.util.Arrays.copyOf(that.findLoss, that.findLossN)
        else if (findLossN + that.findLossN <= findLoss.length) System.arraycopy(findLoss, findLossN, that.findLoss, 0, that.findLossN)
        else {
          findLoss = java.util.Arrays.copyOf(findLoss, math.max(findLossN + (findLossN >> 1) + 2, findLossN + that.findLossN))
          System.arraycopy(findLoss, findLossN, that.findLoss, 0, that.findLossN)
        }
        findLossN += that.findLossN
      }
      this
    }
    private[this] def addStimString(sb: JStringBuilder, prefix: String): JStringBuilder = 
      if (stimuli == 0) sb
      else sb append prefix append Entry.bitsToStimString(stimuli)
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
        c4(t, e.t) &&
        n == e.n && goodN == e.goodN && c2(persist, e.persist) &&
        c3(speed, e.speed) && c4(angSpeed, e.angSpeed) &&
        c2(length, e.length) && c4(relLength, e.relLength) &&
        c2(width, e.width) && c4(relWidth, e.relWidth) &&
        c4(aspect, e.aspect) && c4(relAspect, e.relAspect) &&
        c4(wiggle, e.wiggle) && c2(pixels, e.pixels) &&
        stimuli == e.stimuli &&
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
      val stimuli =
        if ((x ne null) && x == "%") {
          g.skip
          val sb = new JStringBuilder
          while ({x = g.peekTok; (x ne null) && !x.startsWith("%") && !x.startsWith("@")}) {
            g.skip
            if (sb.length > 0) sb append ' '
            sb append x
          }
          stimStringToBits(sb.toString)
        }
        else 0L
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
      entry.n         = n
      entry.goodN     = goodN
      entry.persist   = persist
      entry.speed     = speed
      entry.angSpeed  = angSpeed
      entry.length    = length
      entry.relLength = relLength
      entry.width     = width
      entry.relWidth  = relWidth
      entry.aspect    = aspect
      entry.relAspect = relAspect
      entry.wiggle    = wiggle
      entry.pixels    = pixels
      entry.stimuli   = stimuli
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

  def from(lines: Vector[String]): Ok[String, Summary] = myFrom(lines)(new Grokker{
    def title = "summary"
    def zero() = new Summary()
    def grok(g: Grok)(implicit gh: GrokHop[g.type]) = Entry.parse(g)
  })
}
