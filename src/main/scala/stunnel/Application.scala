package stunnel

import cats.effect.{IO, IOApp}
import cats.effect.kernel.Ref
import cats.effect.std.Env
import cats.implicits.*
import cats.effect.unsafe.IORuntime

import org.http4s.ember.client.EmberClientBuilder
import fs2.Stream

import stunnel.njtransit.{Pattern, VehicleLocation}
import stunnel.njtransit.api.*
import stunnel.geometry.{given, *}
import stunnel.geometry.GeoUtils.pointsCrossed

import scala.concurrent.duration.*
import stunnel.concurrent.{KeyedCache, ConcurrentKeyedCache}

object Application extends IOApp.Simple {

  def getRoutes: IO[List[String]] = Env[IO].get("STUNNEL_ROUTES").map(r => r.getOrElse("").split(',').toList)

  def buildStream(client: Http4sMyBusNowApiClient[IO], patternCache: KeyedCache[String, Pattern]): Stream[IO, Unit] = {
    val prevLocations: java.util.concurrent.ConcurrentMap[Int, Ref[IO, Option[VehicleLocation]]] = new java.util.concurrent.ConcurrentHashMap()

    Stream.awakeEvery[IO](10.seconds)
      .evalMap { _ =>
        getRoutes.flatMap { routes => routes.map(r => client.getVehicles(r)).sequence }
      }
      .flatMap { vehiclesOfRoutes => 
        vehiclesOfRoutes.foldLeft(Stream.empty[IO].asInstanceOf[Stream[IO, VehicleLocation]]) { case (s, vehicles) =>
          s ++ Stream.emits(vehicles)
        }
      }
      .evalMap { vecLoc =>
        prevLocations.computeIfAbsent(vecLoc.tripId, _ => Ref.unsafe[IO, Option[VehicleLocation]](None))
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
            patternCache.loadNoExpiry(y.route)(client.getPatterns(y.route).map(_.head)).flatMap { pattern =>
              val arrivedStops = pattern.pointsCrossed(movement).filter(_.isStop)
              if (arrivedStops.nonEmpty) {
                IO.println(s"[${y.route}] Vehicle ${y.vehicleId} (T${y.tripId}) passed stop(s) ${arrivedStops.mkString(", ")}")
              } else IO.unit
            }
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

      (for {
        routes <- getRoutes
        _ <- IO.println(s"Routes: $routes")
        stream = buildStream(apiClient, patternCache)
      } yield stream.compile.drain).flatten

    }.void
  }
}
