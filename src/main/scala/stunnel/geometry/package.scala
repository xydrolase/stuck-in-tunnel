package stunnel

import stunnel.njtransit.Point
import stunnel.njtransit.VehicleLocation
import org.locationtech.jts.geom.{Coordinate, LineSegment}

package geometry {
  trait CoordOf[T] {
    protected def latOf(p: T): Double 
    protected def lonOf(p: T): Double

    def coordinateOf(p: T): Coordinate = {
      val (x: Double, y: Double) = GeoUtils.mercatorLatLonToMeters(latOf(p), lonOf(p))
      new Coordinate(x, y)
    }

    def lineBetween(p1: T, p2: T): LineSegment = new LineSegment(
      coordinateOf(p1), coordinateOf(p2) 
    )
  }

  object CoordOf {
    def apply[T: CoordOf]: CoordOf[T] = summon
  }

  given CoordOf[Point] with
    protected def latOf(p: Point): Double = p.latitude.toDouble
    protected def lonOf(p: Point): Double = p.longitude.toDouble

  given CoordOf[VehicleLocation] with
    protected def latOf(p: VehicleLocation): Double = p.latitude.toDouble
    protected def lonOf(p: VehicleLocation): Double = p.longitude.toDouble
}

