package stunnel

import cats.effect.{IO, IOApp}
import cats.effect.kernel.{Clock, Ref}
import cats.effect.std.{Env, Queue}
import cats.implicits.*
import cats.effect.unsafe.IORuntime

import org.http4s.ember.client.EmberClientBuilder
import fs2.{Stream, Pipe}
import fs2.io.file.Files
import fs2.aws.s3.S3
import fs2.aws.s3.models.Models.BucketName
import io.laserdisc.pure.s3.tagless.{Interpreter => S3Interpreter}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.regions.Region
import eu.timepit.refined.types.string.NonEmptyString

import stunnel.njtransit.*
import stunnel.njtransit.api.*
import stunnel.config.*
import stunnel.geometry.{given, *}
import stunnel.geometry.GeoUtils.pointsCrossed

import scala.concurrent.duration.*
import stunnel.concurrent.{KeyedCache, ConcurrentKeyedCache}
import java.time.LocalDateTime
import java.time.Instant
import java.time.ZoneOffset
import stunnel.persist.ObjectKeyMaker

object Application extends IOApp.Simple {
  val BatchSize = 100
  val FileLimit = 500
  val MaxTrips = 500

  def getRoutes: IO[List[String]] = Env[IO].get("STUNNEL_ROUTES").map(r => r.getOrElse("").split(',').toList)

  def buildStream(config: AppConfig, client: Http4sMyBusNowApiClient[IO],
                  patternCache: KeyedCache[String, Pattern],
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
          vehicles <- config.tracking.routes.map(r => getVehicles(r)).sequence
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
        // TODO: switch to log4cats
        IO.println(s"[${vec.route}] Vehicle ${vec.vehicleId} (T${vec.tripId}) passed stop: ${arrival.stop}")
      }
      .through(Ops.publishToQueue[BusArrival](busArrivalQueue))
      .void
  }

  def program(config: AppConfig, apiClient: Http4sMyBusNowApiClient[IO], s3: S3[IO]): IO[Unit] = {
    val patternCache = new ConcurrentKeyedCache[String, Pattern]()
    val keyMaker = ObjectKeyMaker.datedWithPrefixSubdir(Some("raw_data"), delimiter = "_")

    for {
      _ <- IO.println(s"Routes: ${config.tracking.routes}")
      vecLocQueue <- Queue.unbounded[IO, VehicleLocation]
      busArrivalQueue <- Queue.unbounded[IO, BusArrival]

      producerStream = buildStream(config, apiClient, patternCache, vecLocQueue, busArrivalQueue)

      // TODO: create a function to generalize the stream construction
      // TODO: parameterize the max queue size
      vecLocWriterStream = Stream.fromQueueUnterminated(vecLocQueue, 16)
        .through(
          Ops.writeArrowRotate[VehicleLocation](
            Files[IO].tempFile(None, "vecloc_", ".arrow", permissions = None).use(p => IO.pure(p)),
            FileLimit, BatchSize
          ))
        .through(Ops.uploadToS3(s3, config.persist.bucketName, keyMaker))

      arrivalWriterStream = Stream.fromQueueUnterminated(busArrivalQueue, 16)
        .through(
          Ops.writeArrowRotate[BusArrival](
            Files[IO].tempFile(None, "arrival_", ".arrow", permissions = None).use(p => IO.pure(p)),
            FileLimit, BatchSize
          ))
        .through(Ops.uploadToS3(s3, config.persist.bucketName, keyMaker))

      result <- Stream(
        producerStream,
        vecLocWriterStream.merge(arrivalWriterStream).evalTap { path =>
          IO.println(s"## File uploaded to S3: ${path.toString}")
        }
      ).parJoin(2).compile.drain

    } yield result 
  }

  def run: IO[Unit] = {
    val keyProvider = StaticKeyProvider[IO]("", "")
    val persistenceConfig = PersistenceConfig(BucketName(NonEmptyString.unsafeFrom("stunnel-bus-data")), Region.US_EAST_1)

    val resource = for {
      apiClient <- EmberClientBuilder.default[IO].build.map { client => 
        new Http4sMyBusNowApiClient[IO](client, keyProvider)
      }
      s3Resource <- S3Interpreter[IO].S3AsyncClientOpResource(
        S3AsyncClient.builder()
          .region(Region.US_EAST_1)
        ).map(ops => S3.create[IO](ops))

    } yield (apiClient, s3Resource)

    for {
      routes <- getRoutes
      // TODO: materialize AppConfig from config file / parameter store
      config = AppConfig(
        EncodingConfig(BatchSize, FileLimit),
        TrackingConfig(routes, MaxTrips),
        persistenceConfig
      )
      prog <- resource.use { (client, s3) =>
        program(config, client, s3)
      }
    } yield prog
  }
}
