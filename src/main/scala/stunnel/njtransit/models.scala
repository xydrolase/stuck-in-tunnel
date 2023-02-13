package stunnel.njtransit

import cats.effect.kernel.Concurrent
import io.circe.Decoder
import io.circe.ACursor
import org.http4s.EntityDecoder
import org.http4s.circe.jsonOf

import java.time.Instant

def prepareDecoder(fieldName: String): ACursor => ACursor = (cur: ACursor) => {
  cur.downField("bustime-response").downField(fieldName)
}

/**
  * Describes the current location of a vehicle. This is returned by the /bustime/api/v3/getvehicles API.
  *
  * @param vehicleId
  * @param timestamp
  * @param latitude
  * @param longitude
  * @param heading
  * @param route
  * @param destination
  * @param tripId
  */
case class VehicleLocation(vehicleId: Int, timestamp: Instant, latitude: Float, longitude: Float,
  heading: Int, route: String, destination: String, tripId: Int)

case class Route()

sealed trait PointType
case object Waypoint extends PointType
case object Stop extends PointType

/**
  * Represents a point in a pattern (route).
  * 
  *    {
  *      "seq": 1,
  *        "lat": 40.735589999998716,
  *        "lon": -74.0291100000008,
  *        "typ": "S",
  *        "stpid": "20496",
  *        "stpnm": "HOBOKEN TERMINAL",
  *        "pdist": 0.0
  *    },
  *    {
  *      "seq": 2,
  *      "lat": 40.7355,
  *      "lon": -74.02945,
  *      "typ": "W",
  *      "pdist": 0.0
  *    } 
  *
  * @param seqNum
  * @param latitude
  * @param longitude
  * @param pointType Whether the point is a waypoint (W) or a stop (S).
  * @param stopId
  * @param stopName
  * @param distance
  */
case class Point(seqNum: Int, latitude: Float, longitude: Float, pointType: PointType,
                 stopId: Option[Int], stopName: Option[String], distance: Float)
object Point:
  given Decoder[Point] = Decoder.forProduct7(
    "seq", "lat", "lon", "typ", "stpid", "stpnm", "pdist") {
      (seqNum: Int, latitude: Float, longitude: Float, pointType: String, stopId: Option[String], stopName: Option[String], distance: Float) =>
        val pt = pointType match {
          case "W" => Waypoint
          case "S" => Stop
          // fallback
          case _ => Waypoint
        }
        Point(seqNum, latitude, longitude, pt, stopId.map(_.toInt), stopName, distance)
    }

case class Pattern(id: Int, routeDirection: String, points: Seq[Point])

object Pattern:
  given Decoder[Pattern] = Decoder.forProduct3("pid", "rtdir", "pt") {
      (id: Int, routeDirection: String, points: Seq[Point]) =>
        Pattern(id, routeDirection, points)
    }

  given Decoder[Seq[Pattern]] = Decoder.decodeSeq.prepare(prepareDecoder("ptr"))

  given [F[_]: Concurrent]: EntityDecoder[F, Seq[Pattern]] = jsonOf

    
