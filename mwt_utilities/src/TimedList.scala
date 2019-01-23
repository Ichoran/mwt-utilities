package mwt.utilities

import java.lang.{ StringBuilder => JStringBuilder }

import kse.flow._
import kse.maths._
import kse.eio._


trait TimedElement {
  def t: Double
}

trait TimedList[E >: Null <: TimedElement] {
  protected def myDefaultSize: Int
  protected var myLines: Array[TimedElement] = null
  protected var myLinesN: Int = 0
  protected var myExtra = collection.mutable.TreeMap.empty[Double, E]

  def pack(): this.type = {
    if (myExtra.size > 0) {
      val ex = {
        val temp = new Array[TimedElement](myExtra.size)
        var i = 0
        myExtra.valuesIterator.foreach{ e => temp(i) = e; i += 1 }
        temp
      }
      myExtra.clear
      val m = myLinesN + ex.length
      if (myLines eq null) {
        myLines = ex
        myLinesN = ex.length
      }
      else if (m <= myLines.length) {
        var i = myLinesN - 1
        var j = ex.length - 1
        var k = m - 1
        while (k > i) {
          myLines(k) =
            if (i < 0)                       { val ans = ex(j);      j -= 1; ans }
            else if (ex(j).t > myLines(i).t) { val ans = ex(j);      j -= 1; ans }
            else                             { val ans = myLines(i); i -= 1; ans }
          k -= 1
        }
      }
      else {
        val n = math.max(myLines.length + (myLines.length >> 2), m + (m >> 2))
        val old = myLines
        myLines = new Array[TimedElement](n)
        var i = 0
        var j = 0
        var k = 0
        while (k < m) {
          myLines(k) =
            if (i >= myLinesN)            { val ans = ex(j);  j += 1; ans }
            else if (j >= ex.length)      { val ans = old(i); i += 1; ans }
            else if (ex(j).t >= old(i).t) { val ans = old(i); i += 1; ans }
            else                          { val ans = ex(j);  j += 1; ans }
          k += 1
        }
      }
      myLinesN = m
    }
    if (myLines ne null) {
      if (myLinesN + (myLinesN >> 2) < myLines.length) {
        myLines = java.util.Arrays.copyOf(myLines, myLinesN)
      }
    }
    this
  }

  protected def mySeek(t: Double): E = {
    if (myLinesN > 0) {
      var i = 0
      var j = myLinesN - 1
      while (i < j) {
        val k = (i + j) >> 1;
        val lkt = myLines(k).t
        if (lkt == t)     return myLines(k).asInstanceOf[E]
        else if (lkt < t) i = k+1
        else              j = k-1
      }
      if (i == j && myLines(i).t == t) return myLines(i).asInstanceOf[E]
    }
    if (myExtra.nonEmpty) myExtra.getOrElse(t, null)
    else null
  }

  protected def myAddMissing(e: E): E = {
    if (myExtra.size >= (if (myLines eq null) myDefaultSize else math.max(myDefaultSize, myLines.length >> 1))) pack()
    if ((myLines eq null) || (myLinesN <= 0 || myLines(myLinesN-1).t < e.t)) {
      if (myLines eq null) myLines = new Array[TimedElement](myDefaultSize)
      else if (myLinesN >= myLines.length) myLines = java.util.Arrays.copyOf(myLines, myLines.length*2)
      myLines(myLinesN) = e
      myLinesN += 1
    }
    else myExtra(e.t) = e
    e
  }

  def add(e: E): E = myAddMissing(e)

  def get(t: Double):  Option[E] = Option(mySeek(t))

  def length: Int = myLinesN + myExtra.size

  def apply(i: Int): E = {
    if (myExtra.size > 0) pack()
    myLines(i).asInstanceOf[E]
  }

  def at(t: Double): E = mySeek(t)
}

trait TimedMonoidList[E >: Null <: TimedElement] extends TimedList[E] {
  protected def emptyElement(t: Double): E
  protected def mutableMerge(existing: E, novel: E): Unit

  override def add(e: E): E = {
    val existing = mySeek(e.t)
    if (existing ne null) { mutableMerge(existing, e); existing }
    else myAddMissing(e)
  }

  override def at(t: Double): E = {
    mySeek(t) ReturnIf (_ ne null)
    emptyElement(t) tap (e => myAddMissing(e))
  }
}

trait TimedListCompanion {
  type MyElement >: Null <: TimedElement
  type MyTimed <: TimedList[MyElement]
  type Grokker = TimedListCompanion.GrokTo[MyElement, MyTimed]

  protected def myFrom(lines: Vector[String])(grokker: Grokker): Ok[String, MyTimed] = {
    val c = grokker.zero()
    val g = Grok("")
    var n = 0
    g.delimit(true, 0)
    g{ implicit fail => 
      lines.foreach{ line =>
        n += 1
        if (line.nonEmpty && !line.startsWith("#")) {
          g.input(line)
          c add grokker.grok(g)(fail)
        }
      }
      c.pack()
    }.mapNo(e => s"Could not parse ${grokker.title} on line $n\n$e")
  }  
}
object TimedListCompanion {
  trait GrokTo[A, Z] {
    def title: String
    def zero(): Z
    def grok(g: Grok)(implicit gh: GrokHop[g.type]): A
  }
}
