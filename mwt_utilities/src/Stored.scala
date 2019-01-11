package mwt.utilities


sealed abstract class Stored {}
object Stored {
  final case class Text(lines: Vector[String]) extends Stored {}
  final case class Binary(data: Array[Byte]) extends Stored {}
}

sealed abstract class FromStore[+Z] {}
object FromStore {
  trait Text[+Z] extends FromStore[Z] { def apply(t: Stored.Text): Z }
  object Text{ def apply[Z](t: Text[Z]) = t }

  trait Binary[+Z] extends FromStore[Z] { def apply(b: Stored.Binary): Z }
  object Binary{ def apply[Z](b: Binary[Z]) = b }

  trait All[+Z] extends Text[Z] with Binary[Z] {
    def from(s: Stored): Z
    final def apply(s: Stored): Z = from(s)
    final def apply(t: Stored.Text) = from(t)
    final def apply(b: Stored.Binary) = from(b)
  }
  object All{ def apply[Z](a: All[Z]) = a }
}
