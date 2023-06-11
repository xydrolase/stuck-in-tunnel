package stunnel

import fs2.{Stream, Pipe}
import cats.effect.IO
import cats.effect.kernel.Ref
import org.locationtech.jts.geom.LineSegment

import stunnel.geometry.{given, *}
import stunnel.njtransit.VehicleLocation
import fs2.Pull

/**
 * Define operations to transform streams to be used by the application.
 */
object Ops {
  opaque type VehicleMovement = (VehicleLocation, VehicleLocation)
  object VehicleMovement:
    def apply(vl1: VehicleLocation, vl2: VehicleLocation): VehicleMovement = (vl1, vl2)

  extension (vm: VehicleMovement) {
    def movement: LineSegment = CoordOf[VehicleLocation].lineBetween(vm._1, vm._2)
    def currentLocation: VehicleLocation = vm._2
  }

  // a simple LRU cache (not thread-safe)
  class LRUCache[K, V](maxEntries: Int, initialSize: Int = 32, loadFactor: Float = 0.75f) 
      extends java.util.LinkedHashMap[K, V](initialSize, loadFactor, true) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[K, V]): Boolean = size > maxEntries
  }

  /**
   * Build a [[Pipe]] that tracks vehicle movments based on the `tripId` of each [[VehicleLocation]] element
   * in the input stream. If a previous location of the same trip is found, a [[VehicleMovement]] element will be 
   * generated in the output stream.
   */
  def trackMovement(maxCacheSize: Int): Pipe[IO, VehicleLocation, VehicleMovement] = {
    def go(s: Stream[IO, VehicleLocation],
           cache: LRUCache[Int, Ref[IO, Option[VehicleLocation]]]): Pull[IO, VehicleMovement, Unit] = {
      s.pull.uncons1.flatMap {
        case None => Pull.done
        case Some((hd, tl)) => 
          // for each head element `hd`, check if we can find the previous location of the same vehicle on the
          // same trip; return `Pull.done` if no previous location can be found (e.g. first known location of
          // the trip), which effectively discards the current `hd` element in the output stream.
          val p = Pull
            .eval {
              // do we need a Ref here? can't we not directly update the value in the Map, esp. if we
              // don't care about thread safety?
              cache.computeIfAbsent(hd.tripId, _ => Ref.unsafe[IO, Option[VehicleLocation]](None))
                .getAndUpdate(_ => Some(hd))
            }
            .flatMap { 
              case None => Pull.done
              case Some(prevLoc) => Pull.output1(VehicleMovement(prevLoc, hd))
            }
          p >> go(tl, cache)
      }
    }

    in => go(in, new LRUCache(maxCacheSize)).stream
  }
}
