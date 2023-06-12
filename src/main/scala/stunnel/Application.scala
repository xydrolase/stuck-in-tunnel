package stunnel

import cats.effect.{IO, IOApp}
import cats.effect.kernel.Ref
import cats.effect.std.{Env, Queue}
import cats.implicits.*
import cats.effect.unsafe.IORuntime

import org.http4s.ember.client.EmberClientBuilder
import fs2.{Stream, Pipe}

import stunnel.njtransit.{Pattern, VehicleLocation}
import stunnel.njtransit.api.*
import stunnel.geometry.{given, *}
import stunnel.geometry.GeoUtils.pointsCrossed

import scala.concurrent.duration.*
import stunnel.concurrent.{KeyedCache, ConcurrentKeyedCache}
import fs2.io.file.Files

object Application extends IOApp.Simple {
  val BatchSize = 20
  val FileLimit = 100

  def getRoutes: IO[List[String]] = Env[IO].get("STUNNEL_ROUTES").map(r => r.getOrElse("").split(',').toList)

  def buildStream(client: Http4sMyBusNowApiClient[IO], patternCache: KeyedCache[String, Pattern],
                  vecLocQueue: Queue[IO, VehicleLocation]): Stream[IO, Unit] = {
    Stream.awakeEvery[IO](10.seconds)
      .evalMap { _ =>
        getRoutes.flatMap { routes => routes.map(r => client.getVehicles(r)).sequence }
      }
      .flatMap { vehiclesOfRoutes => 
        vehiclesOfRoutes.foldLeft(Stream.empty[IO].asInstanceOf[Stream[IO, VehicleLocation]]) { case (s, vehicles) =>
          s ++ Stream.emits(vehicles)
        }
      }
      .through(Ops.publishVehicleLocation(vecLocQueue))
      // track at most 500 trips at once (with an LRU cache)
      .through(Ops.trackMovement(500))
      .evalTap { vecMove =>
        val vec = vecMove.currentLocation
        patternCache.loadNoExpiry(vec.route)(client.getPatterns(vec.route).map(_.head)).flatMap { pattern =>
          val arrivedStops = pattern.pointsCrossed(vecMove.movement).filter(_.isStop)
          if (arrivedStops.nonEmpty) {
            IO.println(s"[${vec.route}] Vehicle ${vec.vehicleId} (T${vec.tripId}) passed stop(s) ${arrivedStops.mkString(", ")}")
          } else IO.unit
        }
      }
      .void
  }

  def run: IO[Unit] = {
    val keyProvider = StaticKeyProvider[IO]("", "")
    val emberClient = EmberClientBuilder.default[IO].build
    val clock = new Clock {
      override def setOffset(offset: Long): Unit = ()
      override def currentTimeMillis: Long = System.currentTimeMillis()
    }

    emberClient.use { client =>
      val apiClient = new Http4sMyBusNowApiClient[IO](client, clock, keyProvider)
      val patternCache = new ConcurrentKeyedCache[String, Pattern]()

      for {
        routes <- getRoutes
        _ <- IO.println(s"Routes: $routes")
        vecLocQueue <- Queue.unbounded[IO, VehicleLocation]

        producerStream = buildStream(apiClient, patternCache, vecLocQueue)
        writerStream = Stream.fromQueueUnterminated(vecLocQueue, 16).through(
          Ops.writeArrowRotate[VehicleLocation](
            Files[IO].tempFile(None, "stunnel_vec_loc_", ".arrow", permissions = None).use(p => IO.pure(p)),
            FileLimit, BatchSize
          )
        ).evalTap { path =>
          IO.println(s"File rotated: ${path.toString}")
        }

        result <- Stream(
          producerStream,
          writerStream
        ).parJoin(2).compile.drain

      } yield result 
    }.void
  }
}
