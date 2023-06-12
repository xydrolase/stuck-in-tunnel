package stunnel.njtransit

import cats.effect.kernel.Concurrent
import cats.Show
import io.circe.{Decoder, ACursor}
import org.http4s.EntityDecoder
import org.http4s.circe.jsonOf

import java.time.{LocalTime, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import sarrow.FieldTypeOf
import stunnel.geometry.GeoUtils

def prepareDecoder(fieldName: String): ACursor => ACursor = (cur: ACursor) => {
  cur.downField("bustime-response").downField(fieldName)
}

/**
  * Describes the current location of a vehicle. This is returned by the /bustime/api/v3/getvehicles API.
  * See: http://realtime.ridemcts.com/bustime/apidoc/docs/DeveloperAPIGuide3_0.pdf
  *
  * {
  *    "vid": "5254",
  *    "tmstmp": "20230214 01:40",
  *    "lat": "40.76042175292969",
  *    "lon": "-74.02761840820312",
  *    "hdg": "200",
  *    "pid": 519,
  *    "rt": "126",
  *    "des": "126 HOBOKEN-PATH",
  *    "pdist": 13707,
  *    "dly": false,
  *    "spd": 11,
  *    "tatripid": "16",
  *    "origtatripno": "19045150",
  *    "tablockid": "126GV019",
  *    "zone": "",
  *    "mode": 0,
  *    "psgld": "EMPTY",
  *    "srvtmstmp": "20230214 01:40",
  *    "oid": "549130",
  *    "or": false,
  *    "rid": "8",
  *    "blk": 157179502,
  *    "tripid": 3897020,
  *    "tripdyn": 0,
  *    "stst": 91800,
  *    "stsd": "2023-02-13"
  *   },
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
case class VehicleLocation(vehicleId: Int, timestamp: LocalDateTime, latitude: Float, longitude: Float,
  patternId: Int, route: String, heading: Int, speed: Int, destination: String, delayed: Boolean, 
  passengerLoad: String, tripId: Int, scheduledStartDt: LocalDateTime) {

    def sameLocationAs(v2: VehicleLocation): Boolean = {
      latitude == v2.latitude && longitude == v2.longitude
    }

    def position: (Double, Double) = {
      GeoUtils.mercatorLatLonToMeters(latitude.toDouble, longitude.toDouble)
    }
}

object VehicleLocation:
  val tsFormatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm")

  given Decoder[VehicleLocation] = Decoder.forProduct14(
    "vid", "tmstmp", "lat", "lon", "pid", "rt", "hdg", "spd", "des", "dly", "psgld", "tripid", "stst", "stsd") {
      (vehicleId: String, timestamp: String, lat: String, lng: String, patternId: Int, route: String, 
        heading: String, speed: Int, destination: String, delayed: Boolean, passengerLoad: String, tripId: Int,
        schedStartTime: Long, schedStartDate: String) =>

        // schedStartTime is technically seconds since the start of the day, but it could be
        // over 24 hours since the start of the day (for late-night buses, since usually the "first" bus starts
        // early in the morning). So we need to check if it's over 24 hours, and if so, add a day to the
        // parsed date.
        val (dayShift: Int, secondOfDay: Long) = if (schedStartTime > 86399) {
          (1, schedStartTime - 86400L)
        } else (0, schedStartTime)
        
        val scheduledDt = LocalTime.ofSecondOfDay(secondOfDay).atDate(
          LocalDate.parse(schedStartDate, DateTimeFormatter.ISO_LOCAL_DATE).plusDays(dayShift))

        VehicleLocation(vehicleId.toInt, LocalDateTime.parse(timestamp, tsFormatter), lat.toFloat, lng.toFloat,
          patternId, route, heading.toInt, speed, destination, delayed, passengerLoad, tripId, scheduledDt)
    }

  given Decoder[Seq[VehicleLocation]] = Decoder.decodeSeq.prepare(prepareDecoder("vehicle"))

  given [F[_]: Concurrent]: EntityDecoder[F, Seq[VehicleLocation]] = jsonOf

/**
 * Describes a bus route.
 *
 * Example JSON:
 * {{{
 *   {
 *     "rt": "1",
 *     "rtnm": "1 Newark",
 *     "rtclr": "#ff3366",
 *     "rtdd": "1"
 *   },
 *   {
 *     "rt": "2",
 *     "rtnm": "2 Jersey City-JournalSq-Secaucus",
 *     "rtclr": "#ff6600",
 *     "rtdd": "2"
 *   }
 * }}}
 *
 * @param route The alphanumeric designator of a route.
 * @param name Common name of the route.
 * @param color Color of the route line used in map.
 */
case class Route(route: String, name: String, color: String)
object Route {
  given Decoder[Route] = Decoder.forProduct3("rt", "rtnm", "rtclr") {
    (route: String, name: String, color: String) => Route(route, name, color)
  }

  given Decoder[Seq[Route]] = Decoder.decodeSeq
  given [F[_]: Concurrent]: EntityDecoder[F, Seq[Route]] = jsonOf
}

sealed trait PointType
case object Waypoint extends PointType
case object Stop extends PointType

object PointType {
  given Show[PointType] with
    def show(pt: PointType): String = pt match
      case Waypoint => "W"
      case Stop => "S"

  given FieldTypeOf[PointType] = FieldTypeOf.withShowWriter
}

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
                 stopId: Option[Int], stopName: Option[String], distance: Float) {
  def isStop: Boolean = pointType == Stop
}

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

case class Pattern(id: Int, routeDirection: String, points: IndexedSeq[Point])


object Pattern:
  given Decoder[Pattern] = Decoder.forProduct3("pid", "rtdir", "pt") {
      (id: Int, routeDirection: String, points: Seq[Point]) =>
        Pattern(id, routeDirection, points.toVector)
    }

  given Decoder[Seq[Pattern]] = Decoder.decodeSeq.prepare(prepareDecoder("ptr"))

  given [F[_]: Concurrent]: EntityDecoder[F, Seq[Pattern]] = jsonOf

    
