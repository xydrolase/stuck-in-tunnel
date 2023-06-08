package stunnel.geometry

import stunnel.njtransit.Pattern
import org.locationtech.jts.geom.{Coordinate, LineSegment}
import org.locationtech.jts.math.Vector2D
import stunnel.njtransit.Point

object GeoUtils {
  opaque type Gate = (LineSegment, LineSegment)

  val Radius: Double = 6378137.0
  val RadianToDegree: Double = math.Pi / 180.0
  /**
   * Convert from lat/lon to meters using Transverse Mercator projection.
   */
  def mercatorLatLonToMeters(lat: Double, lon: Double): (Double, Double) = {
    val x = lon * Radius * RadianToDegree 
    val sin = math.sin(lat * RadianToDegree)
    val y = 0.5 * Radius * math.log((1.0 + sin) / (1.0 - sin))

    (x, y)
  }

  def inverseMercator(x: Double, y: Double): (Double, Double) = {
    val lon = x / Radius / RadianToDegree
    val lat = (math.Pi * 0.5 - 2.0 * math.atan(math.exp(y / -Radius))) / RadianToDegree

    (lat, lon)
  }
  
  /**
   * Compute a virtual "gate" along a provided [[LineSegment]]. This virtual gate can be 
   * used to measure whether another [[LineSegment]] representing vehicle movement in a given
   * time frame crosses with the "gate", thus indicating that the vehicle "arrives at" or
   * "passes through" the gate.
   *
   * For a given [[LineSegment]] along which the gate is to be computed, we can find the
   * [[Point]] along the segment for the given `fraction`. Then, a perpendicular line of
   * given width passing through the [[Point]] will be designated as the virutal gate.
   *
   * @param fraction: The segment length fraction used to determine the center of the
   *                  virtual gate.
   * @param width: The width of the virtual gate, in meters.
   *               The default width is 20 meters, considering that typical travel
   *               lane width in US is about 10 feet. So a gate of 20 meters wide should
   *               cover enough travel lanes plus some drifts in GPS coordinates.
   * @param centralAngle If we treat the two legs of the virtual gate like legs of a
   *                     circular sector, this parameter measures the central angle formed
   *                     by the two legs. By default, the central angle is PI (180 deg),
   *                     meaning that the legs span a half circle. Using a central angle
   *                     smaller than PI may be prefered especially for waypoints on a corner
   *                     because it helps to detect intersection (vehicles "passing" the
   *                     virtual gates) more robustly.
   *
   * @return A [[[LineSegment]] of fixed length. By checking if a movement segment crosses
   * with this "gate" segment, we can tell if the vehicle passes the gate or not.
   *
   * @see https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/LineSegment.html
   *
   */
  def virtualGateOf(alongLine: LineSegment,
                    fraction: Double = 0.995, width: Double = 30.0,
                    centralAngle: Double = math.Pi): Gate = {

    def rotateThenNorm(vec: Vector2D, rad: Double): Vector2D = {
      vec.rotate(rad).divide(vec.length)
    }

    val gateCenter = alongLine.pointAlong(fraction)
    // get the vector representing the directional vector of the `alongLine`
    val directionVector = new Vector2D(alongLine.getCoordinate(0), alongLine.getCoordinate(1))
    val rotRad = math.Pi - centralAngle / 2
    
    (
      new LineSegment(
        gateCenter,
        gateCenter + rotateThenNorm(directionVector, rotRad).multiply(width / 2)
      ),

      new LineSegment(
        gateCenter,
        gateCenter + rotateThenNorm(directionVector, -rotRad).multiply(width / 2)
      )
    )
  }

  extension (gate: Gate) {
    /**
     * Test if a [[Gate]] is passed by (vehicle) movement represented by the provided `segment`.
     * @return true if the provided `segment` intersects with the [[Gate]].
     */
    def passedBy(segment: LineSegment): Boolean = {
      Option(gate._1.intersection(segment)).isDefined ||
        Option(gate._2.intersection(segment)).isDefined
    }

    def geoCoordinates:((Double, Double), (Double, Double), (Double, Double)) = {
      (
        // left leg -> center -> right leg
        inverseMercator(gate._1.getCoordinate(1).getX, gate._1.getCoordinate(1).getY),
        inverseMercator(gate._1.getCoordinate(0).getX, gate._1.getCoordinate(0).getY),
        inverseMercator(gate._2.getCoordinate(1).getX, gate._2.getCoordinate(1).getY)
      )
    }
  }

  extension (pattern: Pattern) {
    /**
     * Get [[LineSegment]] from consecutive waypoints in a bus [[Pattern]].
     */
    def segments: Iterator[LineSegment] = {
      pattern.points.sliding(2).map {
        case Seq(pt1, pt2) =>
          CoordOf[Point].lineBetween(pt1, pt2)
      }
    }

    def segmentGates(fraction: Double = 0.995, width: Double = 100.0,
                     centralAngle: Double = math.Pi): Iterator[Gate] = {
      segments.map { seg =>
        virtualGateOf(seg, fraction, width, centralAngle)
      }
    }

    /**
     * Find all waypoints passed by a (vehicle) movement represented by `line` (the line
     * consists of two points representing two locations of the vehicle at different time).
     *
     * Using this method, we can find out which bus stop(s) a vehicle visited during its
     * movement represented by `line`.
     */
    def pointsCrossed(line: LineSegment): Seq[Point] = {
      segmentGates().zipWithIndex.collect { 
        case (gate, index) if gate.passedBy(line) => pattern.points(index + 1)
      }.toSeq
    }
  }

  extension (c: Coordinate) {
    def +(vec: Vector2D): Coordinate = {
      new Coordinate(c.getX + vec.getX, c.getY + vec.getY)
    }
  }
}
