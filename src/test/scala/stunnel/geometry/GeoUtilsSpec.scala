package stunnel.geometry

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import io.circe.Decoder

import stunnel.njtransit.{Pattern, VehicleLocation, Point}
import stunnel.geometry.CoordOf

import scala.util.Try
import stunnel.geometry.GeoUtils.segmentGates

object GeoUtilsSpec {
  def loadResource[T: Decoder](resource: String, paths: List[String] = Nil): T = {
    val is = getClass.getResourceAsStream(resource)
    try {
      val obj = for {
        raw <- Try(scala.io.Source.fromInputStream(is, "UTF-8").mkString).toEither
        json <- io.circe.parser.parse(raw)
        cursor = paths.foldLeft(json.hcursor.asInstanceOf[io.circe.ACursor]) { case (cur, f) => cur.downField(f) }
        parsed <- cursor.as[T]
      } yield parsed

      obj.fold(t => throw t, identity)
    } finally {
      if (is ne null) is.close()
    }
  }
}

class GeoUtilsSpec extends AnyWordSpec with Matchers {
  import GeoUtilsSpec._
  import GeoUtils.pointsCrossed

  "GeoUtils" when {
    "provide extension methods to Pattern" should {
      "allow extraction of points passed by a line segment" in {
        val pattern = loadResource[Seq[Pattern]]("/patterns.json").head
        val pt1 = loadResource[Seq[VehicleLocation]]("/vehicles1.json").apply(1)
        val pt2 = loadResource[Seq[VehicleLocation]]("/vehicles2.json").apply(1)
        val pt3 = loadResource[Seq[VehicleLocation]]("/vehicles3.json").apply(1)

        val movement1 = CoordOf[VehicleLocation].lineBetween(pt1, pt2)
        pattern.pointsCrossed(movement1).exists(_.isStop) shouldBe false

        val movement2 = CoordOf[VehicleLocation].lineBetween(pt1, pt3)
        pattern.pointsCrossed(movement2).exists(_.isStop) shouldBe true
        pattern.pointsCrossed(movement2).find(_.isStop).get.stopName shouldBe Some("WILLOW AVE AT 15TH ST")
      }

      "compute gates" in {
        val pattern = loadResource[Seq[Pattern]]("/patterns.json").head
        val coords = pattern.points.map { p =>
          s"{lat: ${p.latitude}, lng: ${p.longitude}}"
        }
        println(coords.mkString(",\n"))
        pattern.segmentGates(width = 30.0, centralAngle = 0.75 * math.Pi).foreach { gate =>
          println(gate.geoCoordinates)
        }
      }
    }
  }
}
