package mwt.utilities


sealed abstract class Stored {}
object Stored {
  final case class Text(lines: Vector[String]) extends Stored {}
  final case class Binary(data: Array[Byte]) extends Stored {}
}

sealed abstract class FromStore[+Z] {}
object FromStore {
  trait Text[+Z] extends FromStore[Z] { 
    def apply(t: Stored.Text): Z
    final def apply(lines: Vector[String]): Z = apply(new Stored.Text(lines))
  }
  object Text{ def apply[Z](t: Text[Z]) = t }

  trait Binary[+Z] extends FromStore[Z] {
    def apply(b: Stored.Binary): Z
    final def apply(data: Array[Byte]): Z = apply(new Stored.Binary(data))
  }
  object Binary{ def apply[Z](b: Binary[Z]) = b }

  trait All[+Z] extends Text[Z] with Binary[Z] {
    def from(s: Stored): Z
    final def apply(s: Stored): Z = from(s)
    final def apply(t: Stored.Text) = from(t)
    final def apply(b: Stored.Binary) = from(b)
  }
  object All{ def apply[Z](a: All[Z]) = a }
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
