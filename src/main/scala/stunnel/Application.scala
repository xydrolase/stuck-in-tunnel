package stunnel

import cats.effect.{IO, IOApp}
import cats.effect.kernel.{Clock, Ref}
import cats.effect.std.{Env, Queue}
import cats.implicits.*
import cats.effect.unsafe.IORuntime

import org.http4s.ember.client.EmberClientBuilder
import fs2.{Stream, Pipe}
import fs2.io.file.{Path, Files}
import fs2.aws.s3.S3
import fs2.aws.s3.models.Models.{BucketName, ETag}
import io.laserdisc.pure.s3.tagless.{Interpreter => S3Interpreter}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.regions.Region
import eu.timepit.refined.types.string.NonEmptyString
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import stunnel.njtransit.*
import stunnel.njtransit.api.*
import stunnel.config.*
import stunnel.geometry.{given, *}
import stunnel.geometry.GeoUtils.pointsCrossed
import stunnel.concurrent.{KeyedCache, ConcurrentKeyedCache}
import stunnel.persist.ObjectKeyMaker
import sarrow.{SchemaFor, Indexable}

import java.time.{LocalDateTime, Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.nio.file.{Files => NioFiles}
import scala.concurrent.duration.*
import scala.util.control.NonFatal

object Application extends IOApp.Simple {
  val BatchSize = 500
  val FileLimit = 2000
  val MaxTrips = 500

  val timeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  given Logger[IO] = Slf4jLogger.getLogger[IO]

  def getRoutes: IO[List[String]] = Env[IO].get("STUNNEL_ROUTES").map(r => r.getOrElse("").split(',').toList)

  def getTimeString: IO[String] = {
    Clock[IO].realTime
      .map { dur => LocalDateTime.ofInstant(Instant.ofEpochMilli(dur.toMillis), ZoneOffset.UTC) }
      .map { dt => timeFormatter.format(dt) }
  }

  def buildStream(config: AppConfig, client: Http4sMyBusNowApiClient[IO],
                  patternCache: KeyedCache[Int, Option[Pattern]],
                  vecLocQueue: Queue[IO, VehicleLocation],
                  busArrivalQueue: Queue[IO, BusArrival]): Stream[IO, Unit] = {
    def getVehicles(route: String): IO[Seq[VehicleLocation]] = for {
      vehicles <- client.getVehicles(route).handleError {
        // for now, ignore all failed calls
        case NonFatal(_) => Nil
      }
      ts <- Clock[IO].realTime.map { dur => LocalDateTime.ofInstant(Instant.ofEpochMilli(dur.toMillis), ZoneOffset.UTC) }
    } yield vehicles.map(vl => vl.copy(timestamp = ts))

    // get the pattern of a specific route based on the provided pattern ID, as a bus route may have multiple patterns
    // (e.g different directions, or different destionations)
    def getPattern(route: String, patternId: Int): IO[Option[Pattern]] = patternCache.loadNoExpiry(patternId) { 
      client.getPatterns(route).map(patterns => patterns.find(_.id == patternId))
    }

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
        Logger[IO].info(s"[${vec.route}] Vehicle ${vec.vehicleId} (T${vec.tripId}) passed stop: ${arrival.stop}")
      }
      .through(Ops.publishToQueue[BusArrival](busArrivalQueue))
      .void
  }

  def persistData[A: SchemaFor: Indexable](queue: Queue[IO, A],
      prefix: String, config: PersistenceConfig, s3: S3[IO], keyMaker: ObjectKeyMaker): Stream[IO, ETag] = {

    Stream
      // TODO: parameterize the max queue size
      .fromQueueUnterminated(queue, 16) 
      .through(
        Ops.writeArrowRotate[A](
          getTimeString.flatMap { ts =>
            IO.blocking(Path.fromNioPath(NioFiles.createTempFile(s"${prefix}_${ts}_", ".arrow")))
          },
          config.encoding.fileRecordsLimit, config.encoding.batchSize
        )
      )
      .evalTap { path =>
        Logger[IO].info(s"File closed: $path")
      }
      .through(Ops.uploadToS3(s3, config.bucketName, keyMaker))
      .evalTap { etag =>
        Logger[IO].info(s"File uploaded to S3, etag: $etag")
      }
  }

  def program(config: AppConfig, apiClient: Http4sMyBusNowApiClient[IO], s3: S3[IO]): IO[Unit] = {
    val patternCache = new ConcurrentKeyedCache[Int, Option[Pattern]]()
    val keyMaker = ObjectKeyMaker.datedWithPrefixSubdir(Some("raw_data"), delimiter = "_")

    for {
      _ <- IO.println(s"Routes: ${config.tracking.routes}")
      vecLocQueue <- Queue.unbounded[IO, VehicleLocation]
      busArrivalQueue <- Queue.unbounded[IO, BusArrival]

      producerStream = buildStream(config, apiClient, patternCache, vecLocQueue, busArrivalQueue)

      vecLocWriterStream = persistData[VehicleLocation](
        vecLocQueue, "vecloc", config.persistence("location"), s3, keyMaker)

      arrivalWriterStream = persistData[BusArrival](
        busArrivalQueue, "arrival", config.persistence("arrival"), s3, keyMaker)

      result <- Stream(
        producerStream,
        vecLocWriterStream.merge(arrivalWriterStream)
      ).parJoin(2).compile.drain

    } yield result 
  }

  def run: IO[Unit] = {
    val keyProvider = StaticKeyProvider[IO]("", "")
    val persistenceConfig = Map(
      "location" -> PersistenceConfig(EncodingConfig(BatchSize, FileLimit),
        BucketName(NonEmptyString.unsafeFrom("stunnel-bus-data")), Region.US_EAST_1),
      "arrival" -> PersistenceConfig(EncodingConfig(BatchSize / 5, FileLimit / 5),
        BucketName(NonEmptyString.unsafeFrom("stunnel-bus-data")), Region.US_EAST_1)
    )

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
        TrackingConfig(routes, MaxTrips),
        persistenceConfig
      )
      prog <- resource.use { (client, s3) =>
        program(config, client, s3)
      }
    } yield prog
  }
}
