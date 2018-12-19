package mwt.utilities

import java.io._
import java.nio.file._
import java.time._

import scala.collection.JavaConverters._

import kse.flow._
import kse.eio._

final case class OutputTarget(name: String, when: Instant, prefix: Option[String]) {
  def isZip = name endsWith ".zip"
}
object OutputTarget {
  def from(s: String, debug: Boolean = true): Ok[String, OutputTarget] = s.split('_').pipe{ parts =>
    def no(why: => String) = No(if (debug) why else "Misformatted")

    if (parts.length < 2) return no(s"Too few parts (${parts.length})")
    val date = parts(parts.length-2)
    val time = parts(parts.length-1).take(6)
    if (date.length != 8) return no(s"Date chunk is not 8 chars long: $date")
    if (time.length < 6) return no(s"Time chunk is less than 6 chars: $time")
    nFor(date.length){ i => val c = date.charAt(i); if (c < '0' || c > '9') return no("Date char #${i+1} is not a digit: $c") }
    nFor(time.length){ i => val c = time.charAt(i); if (c < '0' || c > '9') return no("Time char #${i+1} is not a digit: $c") }
    val zoned = parts(parts.length-1).pipe{ bit =>
      if (bit.length == 6) false
      else if (bit.length == 7) {
        if (bit.charAt(6) == 'Z') true else return no("Last part right length to end with Z but ends with ${bit.charAt(6)}")
      }
      else if (bit.length == 10) {
        if (bit endsWith ".zip") false else return no("Last part right length to end with .zip but ends with ${bit.drop(6)}")
      }
      else if (bit.length == 11) {
        if (bit endsWith "Z.zip") true else return no("Last part right length to end with Z.zip but ends with ${bit.drop(6)}")
      }
      else return No("Wrong size for last part (${bit.length})")
    }
    val instant = safe {
      val y = date.substring(0, 4).toInt
      val mo = date.substring(4, 6).toInt
      val d = date.substring(6, 8).toInt
      val h = time.substring(0, 2).toInt
      val min = time.substring(2, 4).toInt
      val sec = time.substring(4, 6).toInt
      Instant from ZonedDateTime.of(y, mo, d, h, min, sec, 0, if (zoned) ZoneOffset.UTC else ZoneId.systemDefault)
    }.mapNo(e => s"Error parsing date:\n${e.explain()}\n").?
    val prefix = (
      if (parts.length > 2) Some(s.dropRight(2 + date.length + parts(parts.length-1).length))
      else None
    )
    Yes(new OutputTarget(s, instant, prefix))
  }

  def from(f: File): Ok[String, OutputTarget] = from(f.getName)
  def from(f: File, debug: Boolean): Ok[String, OutputTarget] = from(f.getName, debug)

  def from(p: Path): Ok[String, OutputTarget] = from(p.getFileName.toString)
  def from(p: Path, debug: Boolean): Ok[String, OutputTarget] = from(p.getFileName.toString, debug)
}
