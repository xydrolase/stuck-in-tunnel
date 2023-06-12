package stunnel

import cats.effect.{IO, IOApp}
import cats.effect.kernel.{Clock, Ref}
import cats.effect.std.{Env, Queue}
import cats.implicits.*
import cats.effect.unsafe.IORuntime

import org.http4s.ember.client.EmberClientBuilder
import fs2.{Stream, Pipe}
import fs2.io.file.Files

import stunnel.njtransit.*
import stunnel.njtransit.api.*
import stunnel.geometry.{given, *}
import stunnel.geometry.GeoUtils.pointsCrossed

import scala.concurrent.duration.*
import stunnel.concurrent.{KeyedCache, ConcurrentKeyedCache}
import java.time.LocalDateTime
import java.time.Instant
import java.time.ZoneOffset

object Application extends IOApp.Simple {
  val BatchSize = 20
  val FileLimit = 100

  def getRoutes: IO[List[String]] = Env[IO].get("STUNNEL_ROUTES").map(r => r.getOrElse("").split(',').toList)

  def buildStream(client: Http4sMyBusNowApiClient[IO], patternCache: KeyedCache[String, Pattern],
                  vecLocQueue: Queue[IO, VehicleLocation],
                  busArrivalQueue: Queue[IO, BusArrival]): Stream[IO, Unit] = {
    def getVehicles(route: String): IO[Seq[VehicleLocation]] = for {
      vehicles <- client.getVehicles(route)
      ts <- Clock[IO].realTime.map { dur => LocalDateTime.ofInstant(Instant.ofEpochMilli(dur.toMillis), ZoneOffset.UTC) }
    } yield vehicles.map(vl => vl.copy(timestamp = ts))

    def getPattern(route: String): IO[Pattern] = patternCache.loadNoExpiry(route)(client.getPatterns(route).map(_.head))

    Stream.awakeEvery[IO](10.seconds)
      .evalMap { _ =>
        for {
          routes <- getRoutes
          vehicles <- routes.map(r => getVehicles(r)).sequence
        } yield vehicles
      }
      .flatMap { vehiclesOfRoutes => 
        vehiclesOfRoutes.foldLeft(Stream.empty[IO].asInstanceOf[Stream[IO, VehicleLocation]]) { case (s, vehicles) =>
          s ++ Stream.emits(vehicles)
        }
      }
      .through(Ops.publishToQueue[VehicleLocation](vecLocQueue))
      // track at most 500 trips at once (with an LRU cache)
      .through(Ops.trackMovement(500))
      .through(Ops.trackBusStopArrivals(getPattern))
      .evalTap { arrival =>
        val vec = arrival.currentLocation
        IO.println(s"[${vec.route}] Vehicle ${vec.vehicleId} (T${vec.tripId}) passed stop: ${arrival.stop}")
      }
      .through(Ops.publishToQueue[BusArrival](busArrivalQueue))
      .void
  }

  def run: IO[Unit] = {
    val keyProvider = StaticKeyProvider[IO]("", "")
    val emberClient = EmberClientBuilder.default[IO].build

    emberClient.use { client =>
      val apiClient = new Http4sMyBusNowApiClient[IO](client, keyProvider)
      val patternCache = new ConcurrentKeyedCache[String, Pattern]()

      for {
        routes <- getRoutes
        _ <- IO.println(s"Routes: $routes")
        vecLocQueue <- Queue.unbounded[IO, VehicleLocation]
        busArrivalQueue <- Queue.unbounded[IO, BusArrival]

        producerStream = buildStream(apiClient, patternCache, vecLocQueue, busArrivalQueue)
        vecLocWriterStream = Stream.fromQueueUnterminated(vecLocQueue, 16).through(
          Ops.writeArrowRotate[VehicleLocation](
            Files[IO].tempFile(None, "stunnel_vec_loc_", ".arrow", permissions = None).use(p => IO.pure(p)),
            FileLimit, BatchSize
          )
        )
        arrivalWriterStream = Stream.fromQueueUnterminated(busArrivalQueue, 16).through(
          Ops.writeArrowRotate[BusArrival](
            Files[IO].tempFile(None, "stunnel_arrival_", ".arrow", permissions = None).use(p => IO.pure(p)),
            FileLimit, BatchSize
          )
        )

        result <- Stream(
          producerStream,
          vecLocWriterStream.merge(arrivalWriterStream).evalTap { path =>
            IO.println(s"## File rotated: ${path.toString}")
          }
        ).parJoin(2).compile.drain

      } yield result 
    }.void
  }
}
