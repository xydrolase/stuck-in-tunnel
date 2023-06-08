package stunnel

import cats.effect.{IO, IOApp}
import cats.effect.kernel.Ref
import org.http4s.ember.client.EmberClientBuilder
import fs2.Stream

import stunnel.njtransit.VehicleLocation
import stunnel.njtransit.api.*
import stunnel.geometry.{given, *}
import stunnel.geometry.GeoUtils.pointsCrossed

import scala.concurrent.duration.*

object Application extends IOApp.Simple {

  def run: IO[Unit] = {
    val keyProvider = StaticKeyProvider[IO]("", "")
    val emberClient = EmberClientBuilder.default[IO].build
    val clock = new Clock {
      override def setOffset(offset: Long): Unit = ()
      override def currentTimeMillis: Long = System.currentTimeMillis()
    }

    emberClient.use { client =>
      val apiClient = new Http4sMyBusNowApiClient[IO](client, clock, keyProvider)
      val prevLocations: java.util.concurrent.ConcurrentMap[Int, Ref[IO, Option[VehicleLocation]]] = new java.util.concurrent.ConcurrentHashMap()

      val patternIO = apiClient.getPatterns("126").map(_.head)
      patternIO.flatMap { pattern =>
        // track vehicle movements between two ticks using vehicle ID
        val vehicles = Stream.awakeEvery[IO](10.seconds)
          .evalMap { _ =>
            apiClient.getVehicles("126")
          }
          .flatMap(Stream.emits)
          .evalMap { vecLoc =>
            prevLocations.computeIfAbsent(vecLoc.vehicleId, _ => Ref.unsafe[IO, Option[VehicleLocation]](None))
              .getAndUpdate(_ => Some(vecLoc))
              .map { prevLoc =>
                (prevLoc, vecLoc)
              }
          }
          .evalTap { case (x, y) =>
            x match {
              case None => IO.unit
              case Some(x) =>
                val movement = CoordOf[VehicleLocation].lineBetween(x, y)
                val arrivedStops = pattern.pointsCrossed(movement).filter(_.isStop)
                if (arrivedStops.nonEmpty) {
                  IO.println(s"Vehicle ${y.vehicleId} [${y.route}] passed stop(s) ${arrivedStops.mkString(", ")}")
                } else IO.unit
            }
          }

        vehicles.compile.drain
      }

    }.void
  }
}
