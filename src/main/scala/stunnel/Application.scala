package stunnel

import cats.effect.IOApp
import cats.effect.IO
import org.http4s.ember.client.EmberClientBuilder
import fs2.Stream

import stunnel.njtransit.api.*

import scala.concurrent.duration.*
import cats.effect.kernel.Ref
import stunnel.njtransit.VehicleLocation

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
          IO.println(s"Prev: $x, Current: $y, " +
            s"Delta(Lat) = ${x.map(_.latitude - y.latitude).getOrElse(Float.NaN)}, Delta(Lng) = ${x.map(_.longitude - y.longitude).getOrElse(Float.NaN)}")
        }

      vehicles.compile.drain
    }.void
  }
}
