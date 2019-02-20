package mwt.utilities


sealed abstract class Stored {}
object Stored {
  sealed trait Data extends Stored {}

  final case class Text(lines: Vector[String]) extends Data {}
  object Text { val empty = new Text(Vector.empty) }

  final case class Binary(data: Array[Byte]) extends Data {}
  object Binary { val empty = new Binary(Array.empty) }

  final case object Empty extends Stored {}
}

sealed abstract class FromStore[+Z] {}
object FromStore {
  sealed trait Transformative {}

  trait Empty[+Z] extends FromStore[Z] {
    def apply(e: Stored.Empty.type): Z
    final def apply(): Z = apply(Stored.Empty)
    final def asIfData[Y](f: Z => Either[Stored.Data, Y], postBin: Binary[Y], postText: Text[Y]): Empty[Y] = e => f(apply(e)) match {
      case Left(t: Stored.Text)   => postText(t)
      case Left(b: Stored.Binary) => postBin(b)
      case Right(y)               => y
    }
  }
  object Empty {
    def to[Z](z: => Z) = new Empty[Z] { def apply(e: Stored.Empty.type) = z }
    def apply[Z](e: Empty[Z]) = e
  }

  trait Duo[+Z] extends FromStore[Z] {
    def apply(t: Stored.Text, b: Stored.Binary): Z
    final def apply(lines: Vector[String], b: Stored.Binary): Z = apply(new Stored.Text(lines), b)
    final def apply(t: Stored.Text, data: Array[Byte]): Z = apply(t, new Stored.Binary(data))
    final def apply(lines: Vector[String], data: Array[Byte]): Z = apply(new Stored.Text(lines), new Stored.Binary(data))
    def branch[Y](f: Z => Either[Stored.Data, Y], postBin: Binary[Y], postText: Text[Y]): Duo[Y] = (txt, bin) => f(apply(txt, bin)) match {
      case Left(t: Stored.Text)   => postText(t)
      case Left(b: Stored.Binary) => postBin(b)
      case Right(y)               => y
    }
  }
  object Duo {
    def apply[Z](d: Duo[Z]) = d
    trait Transform extends Duo[(Stored.Text, Stored.Binary)] {
      final def andThen[Z](that: Duo[Z]): Duo[Z] = (txt, bin) => { val (t, b) = this.apply(txt, bin); that(t, b) }
    }
    object Transform { def apply(t: Transform) = t }
  }

  trait Text[+Z] extends Duo[Z] { 
    def apply(t: Stored.Text): Z
    final def apply(lines: Vector[String]): Z = apply(new Stored.Text(lines))
    @deprecatedOverriding("Overriding this method is for internal use only", "0.2.0")
    def apply(t: Stored.Text, b: Stored.Binary) = apply(t)
    final def branch[Y](f: Z => Either[Stored.Text, Y], post: Text[Y]): Text[Y] = txt => f(apply(txt)) match {
      case Left(t)  => post(t)
      case Right(y) => y
    }
    @deprecatedOverriding("Overriding this method is for internal use only", "0.2.0")
    override def branch[Y](f: Z => Either[Stored.Data, Y], postBin: Binary[Y], postText: Text[Y]): Text[Y] = txt => f(apply(txt)) match {
      case Left(t: Stored.Text)   => postText(t)
      case Left(b: Stored.Binary) => postBin(b)
      case Right(y)               => y
    }
  }
  object Text { 
    def apply[Z](t: Text[Z]) = t
    trait Transform extends Text[Stored.Text] {
      final def andThen[Z](that: Text[Z]): Text[Z] = txt => that.apply(this.apply(txt))
    }
    object Transform { def apply(t: Transform) = t }
  }

  trait Binary[+Z] extends Duo[Z] {
    def apply(b: Stored.Binary): Z
    final def apply(data: Array[Byte]): Z = apply(new Stored.Binary(data))
    final override def apply(t: Stored.Text, b: Stored.Binary) = apply(b)
    final def branch[Y](f: Z => Either[Stored.Binary, Y], post: Binary[Y]): Binary[Y] = bin => f(apply(bin)) match {
      case Left(b)  => post(b)
      case Right(y) => y
    }
    @deprecatedOverriding("Overriding this method is for internal use only", "0.2.0")
    override def branch[Y](f: Z => Either[Stored.Data, Y], postBin: Binary[Y], postText: Text[Y]): Binary[Y] = bin => f(apply(bin)) match {
      case Left(t: Stored.Text)   => postText(t)
      case Left(b: Stored.Binary) => postBin(b)
      case Right(y)               => y
    }
  }
  object Binary { 
    def apply[Z](b: Binary[Z]) = b
    trait Transform extends Binary[Stored.Binary] {
      final def andThen[Z](that: Binary[Z]): Binary[Z] = bin => that.apply(this.apply(bin))
    }
    object Transform { def apply(t: Transform) = t }
  }

  trait All[+Z] extends Text[Z] with Binary[Z] {
    def from(s: Stored.Data): Z
    final def apply(s: Stored.Data): Z = from(s)
    final def apply(t: Stored.Text): Z = from(t)
    final def apply(b: Stored.Binary): Z = from(b)
    final override def branch[Y](f: Z => Either[Stored.Data, Y], postBin: Binary[Y], postText: Text[Y]): All[Y] = data => f(from(data)) match {
      case Left(t: Stored.Text)   => postText(t)
      case Left(b: Stored.Binary) => postBin(b)
      case Right(y)               => y
    }
  }
  object All{ def apply[Z](a: All[Z]) = a }


  def aggregate[Z](channels: collection.Iterable[FromStore[Z]])(zero: => Z, reducer: (Z, Z) => Z): FromStore[Z] = {
    val chs = channels.toArray.zipWithIndex
    if (chs.isEmpty) Empty.to(zero)
    else {
      val empties  = chs.collect{ case (e: FromStore.Empty[Z],  i) => (e, i) }
      val texts    = chs.collect{ case (t: FromStore.Text[Z],   i) => (t, i) }
      val binaries = chs.collect{ case (b: FromStore.Binary[Z], i) => (b, i) }
      val duos     = chs.collect{ case (d: FromStore.Duo[Z],    i) => (d, i) }
      if (duos.isEmpty) Empty(e => empties.map(_._1 apply e).reduce(reducer))
      else if (duos.size == binaries.size) Binary { bin =>
        var ei = 0
        var bi = 0
        var z: Option[Z] = None
        while (ei < empties.length || bi < binaries.length) {
          val zi =
            if (bi >= binaries.length)                 { ei += 1; empties( ei-1)._1.apply(Stored.Empty) }
            else if (ei >= empties.length)             { bi += 1; binaries(bi-1)._1.apply(bin) }
            else if (empties(ei)._2 < binaries(bi)._2) { ei += 1; empties( ei-1)._1.apply(Stored.Empty) }
            else                                       { bi += 1; binaries(bi-1)._1.apply(bin) }
          z = Some(z match {
            case Some(zacc) => reducer(zacc, zi)
            case _          => zi
          })
        }
        z.getOrElse(zero)
      }
      else if (duos.size == texts.size) Text{ txt =>
        var ei = 0
        var ti = 0
        var z: Option[Z] = None
        while (ei < empties.length || ti < texts.length) {
          val zi =
            if (ti >= texts.length)                 { ei += 1; empties(ei-1)._1.apply(Stored.Empty) }
            else if (ei >= texts.length)            { ti += 1; texts(  ti-1)._1.apply(txt) }
            else if (empties(ei)._2 < texts(ti)._2) { ei += 1; empties(ei-1)._1.apply(Stored.Empty) }
            else                                    { ti += 1; texts(  ti-1)._1.apply(txt) }
          z = Some(z match {
            case Some(zacc) => reducer(zacc, zi)
            case _          => zi
          })
        }
        z.getOrElse(zero)
      }
      else Duo { (txt, bin) =>
        var ei = 0
        var di = 0
        var z: Option[Z] = None
        while (ei < empties.length || di < duos.length) {
          val zi =
            if (di >= duos.length)                 { ei += 1; empties(ei-1)._1.apply(Stored.Empty) }
            else if (ei >= duos.length)            { di += 1; duos(  di-1)._1.apply(txt, bin) }
            else if (empties(ei)._2 < duos(di)._2) { ei += 1; empties(ei-1)._1.apply(Stored.Empty) }
            else                                   { di += 1; duos(  di-1)._1.apply(txt, bin) }
          z = Some(z match {
            case Some(zacc) => reducer(zacc, zi)
            case _          => zi
          })
        }
        z.getOrElse(zero)
      }
    }
  }

  def aggregateText[Z](channels: collection.Iterable[Text[Z]])(zero: => Z, reducer: (Z, Z) => Z): Text[Z] = {
    val texts = channels.toArray
    if (texts.isEmpty) Text{ _ => zero }
    else Text{ txt =>
      var ti = 0
      var z: Option[Z] = None
      while (ti < texts.length) {
        val zi = texts(ti).apply(txt)
        ti += 1
        z = Some(z match {
          case Some(zacc) => reducer(zacc, zi)
          case _          => zi
        })
      }
      z.getOrElse(zero)
    }
  }

  def aggregateBinary[Z](channels: collection.Iterable[Binary[Z]])(zero: => Z, reducer: (Z, Z) => Z): Binary[Z] = {
    val binaries = channels.toArray
    if (binaries.isEmpty) Binary{ _ => zero }
    else Binary{ bin =>
      var bi = 0
      var z: Option[Z] = None
      while (bi < binaries.length) {
        val zi = binaries(bi).apply(bin)
        bi += 1
        z = Some(z match {
          case Some(zacc) => reducer(zacc, zi)
          case _          => zi
        })
      }
      z.getOrElse(zero)
    }
  }

  def broadcast(channels: collection.Iterable[FromStore[Unit]]) = aggregate(channels)((), (_, _) => ())

  def broadcastText(channels: collection.Iterable[Text[Unit]]) = aggregateText(channels)((), (_, _) => ())

  def broadcastBinary(channels: collection.Iterable[FromStore.Binary[Unit]]) = aggregateBinary(channels)((), (_, _) => ())
}

object Approximation {
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
  final def r5(x: Double): Double = 
    if (x < 0) { if (x < -1e10) x else (x*100000 - 0.5).toLong/1e5 }
    else       { if (x >  1e10) x else (x*100000 + 0.5).toLong/1e5 }

  final def c1(x: Double, y: Double): Boolean = (x-y).abs < 0.05
  final def c2(x: Double, y: Double): Boolean = (x-y).abs < 0.005
  final def c3(x: Double, y: Double): Boolean = (x-y).abs < 0.0005
  final def c4(x: Double, y: Double): Boolean = (x-y).abs < 0.00005
  final def c5(x: Double, y: Double): Boolean = (x-y).abs < 0.000005
  final def c6(x: Double, y: Double): Boolean = (x-y).abs < 0.0000005  
}
