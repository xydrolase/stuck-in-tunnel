package stunnel.geometry

import stunnel.njtransit.Pattern
import org.locationtech.jts.geom.{Coordinate, LineSegment}
import org.locationtech.jts.math.Vector2D
import stunnel.njtransit.Point

object GeoUtils {
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
   *
   * @return A [[[LineSegment]] of fixed length. By checking if a movement segment crosses
   * with this "gate" segment, we can tell if the vehicle passes the gate or not.
   *
   * @see https://locationtech.github.io/jts/javadoc/org/locationtech/jts/geom/LineSegment.html
   *
   */
  def virtualGateOf(alongLine: LineSegment,
                    fraction: Double = 0.995, width: Double = 30.0): LineSegment = {
    val gateCenter = alongLine.pointAlong(fraction)
    // get the vector representing the directional vector of the `alongLine`
    val directionVector = new Vector2D(alongLine.getCoordinate(0), alongLine.getCoordinate(1))
    // get the norm (unit) vector by rotating 90 degrees
    val normVector = directionVector.rotate(math.Pi / 2).divide(directionVector.length)
    
    new LineSegment(
      gateCenter + normVector.multiply(width / 2),
      gateCenter + normVector.multiply(width / 2).negate()
    )
  }

  opaque type Gate = LineSegment

  extension (gate: Gate) {
    def passedBy(segment: LineSegment): Boolean = {
      Option(gate.intersection(segment)).isDefined
    }

    def geoCoordinates:((Double, Double), (Double, Double)) = {
      (
        inverseMercator(gate.getCoordinate(0).getX, gate.getCoordinate(0).getY),
        inverseMercator(gate.getCoordinate(1).getX, gate.getCoordinate(1).getY)
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

    def segmentGates(fraction: Double = 0.995, width: Double = 100.0): Iterator[Gate] = {
      segments.map { seg =>
        virtualGateOf(seg, fraction, width)
      }
    }

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
