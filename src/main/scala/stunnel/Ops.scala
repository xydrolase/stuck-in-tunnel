package stunnel

import fs2.{Stream, Pipe, Pull, Chunk}
import fs2.io.file.Path
import cats.effect.{IO, Resource}
import cats.effect.kernel.Ref
import cats.effect.std.{Queue, Hotswap}
import org.apache.arrow.vector.ipc.ArrowFileWriter
import fs2.aws.s3.S3
import fs2.aws.s3.models.Models.{BucketName, FileKey, ETag}
import io.laserdisc.pure.s3.tagless.{Interpreter => S3Interpreter}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import java.io.FileOutputStream
import java.nio.channels.Channels

import stunnel.geometry.{given, *}
import stunnel.geometry.GeoUtils.pointsCrossed
import stunnel.njtransit.*
import stunnel.concurrent.KeyedCache
import stunnel.persist.ObjectKeyMaker
import sarrow.{SchemaFor, ArrowWriter, Indexable}

/**
 * Define operations to transform streams to be used by the application.
 */
object Ops {
  case class WriterDelegate[T](writer: ArrowWriter[T], fileWriter: ArrowFileWriter, path: Path) {
    def write(input: T) = writer.write(input)

    def writeBatch(): IO[Unit] = IO.blocking {
      writer.finish()
      fileWriter.writeBatch()
      writer.reset()
    }

    def close(): IO[Unit] = IO.blocking {
      try {
        // if there are still data unwritten to file, flush before closing.
        if (writer.isDirty) {
          writer.finish()
          fileWriter.writeBatch()
          writer.reset()
        }
      } finally {
        fileWriter.close()
      }
    }
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

    // `in` is Stream[IO, VehicleLocation]
    in => go(in, new LRUCache(maxCacheSize)).stream
  }

  def trackBusStopArrivals(computePattern: (String, Int) => IO[Option[Pattern]]): Pipe[IO, VehicleMovement, BusArrival] = {
    in => in.flatMap { vm =>
      val location = vm.currentLocation
      Stream.eval(computePattern(location.route, location.patternId)).flatMap { 
        // if pattern by the given id is found, find all arrivals; otherwise, emit nothing.
        case Some(pattern) => 
          val arrivals = pattern.pointsCrossed(vm.movement).filter(_.isStop).map { stop =>
            BusArrival(location, stop)
          }

          Stream.emits(arrivals)
        case None =>
          Stream.emits(Nil)
      }
    }
  }

  // simple pipe to publish elements of type [[T]] into a queue
  def publishToQueue[T](queue: Queue[IO, T]): Pipe[IO, T, T] = {
    in => in.evalTap { element =>
      queue.offer(element)
    }
  }

  /**
   * A [[Pipe]] that writes incoming streaming elements of type [[T]] to output Arrow IPC files.
   * Each file, once completes, will be emitted in the output stream (for downstream processing, e.g. upload to S3).
   *
   * @param computePath an [[IO]] effect that computes the output path of the arrow file. 
   * @param limit The number of records to be written to an Arrow IPC file before the file rotates.
   * @param batchSize The size of an Arrow record batch. Each batch is held in memory in a [[VectorSchemaRoot]] before
   *                  flushed to the output Arrow IPC file.
   */
  def writeArrowRotate[T: SchemaFor: Indexable](computePath: IO[Path], limit: Int, batchSize: Int): Pipe[IO, T, Path] = {
    val schemaFor = summon[SchemaFor[T]]

    def newFileWriter(writer: ArrowWriter[T]): Resource[IO, WriterDelegate[T]] =
      Resource
        .eval(computePath.flatTap(p => IO.println(s"## Opening file: $p")))
        .flatMap { p => 
          Resource.make(IO.blocking {
            val fos = new FileOutputStream(p.toString)
            val fileWriter = new ArrowFileWriter(writer.root, null, Channels.newChannel(fos))
            fileWriter.start()
            WriterDelegate(writer, fileWriter, p)
          })(delegate => delegate.close())
        }

    def go(writerHotswap: Hotswap[IO, WriterDelegate[T]], delegate: WriterDelegate[T],
           batchAcc: Int, fileAcc: Int, s: Stream[IO, T]): Pull[IO, Path, Unit] = {

      val toWrite = (limit - fileAcc).min(batchSize - batchAcc).min(Int.MaxValue)
      s.pull.unconsLimit(toWrite).flatMap {
        case None => 
          // if there are pending records, make sure they are flushed
          if (delegate.writer.isDirty) Pull.eval(delegate.close()) >> Pull.output1(delegate.path)
          else Pull.done
        case Some((hd, tl)) =>
          val newBatchAcc = batchAcc + hd.size
          val newFileAcc = fileAcc + hd.size

          val append = Pull.eval {
            IO(hd.foreach(row => delegate.write(row)))
          }

          // check if we exceed the batch size, or the file limit
          if (newFileAcc >= limit) {
            append >> 
              // must flush the current batch before closing the file
              Pull.eval(delegate.writeBatch()) >> 
              Pull.eval(IO.blocking(delegate.fileWriter.close())) >>
              // output the current file
              Pull.output1(delegate.path) >> 
              Pull.eval {
                writerHotswap.swap(newFileWriter(delegate.writer))
              }
              .flatMap { d => 
                // check if we need to reset batchAcc as well 
                go(writerHotswap, d, if (newBatchAcc >= batchSize) 0 else newBatchAcc, 0, tl)  
              }
          } else if (newBatchAcc >= batchSize) {
            append >> Pull.eval(delegate.writeBatch()) >> go(writerHotswap, delegate, 0, newFileAcc, tl)
          } else append >> go(writerHotswap, delegate, newBatchAcc, newFileAcc, tl)
      }
    }

    (in: Stream[IO, T]) => for {
      writer <- Stream.resource(Resource.make(IO(schemaFor.buildArrowWriter()))(w => IO(w.reset())))
      (hotswap, delegate) <- Stream.resource(Hotswap(newFileWriter(writer)))
      stream <- go(hotswap, delegate, 0, 0, in).stream
    } yield stream
  }

  def uploadToS3(s3: S3[IO], bucket: BucketName, keyMaker: ObjectKeyMaker,
                 persistLocally: Boolean = false): Pipe[IO, Path, ETag] = {
    (in: Stream[IO, Path]) => {
      in.flatMap { path =>
        val key = keyMaker.createKey(path)
        fs2.io.file.readAll[IO](path.toNioPath, 4096)
          .through(s3.uploadFile(bucket, key))
          .evalTap { _ =>
            if (!persistLocally) fs2.io.file.delete(path.toNioPath) else IO.unit
          }
      }
    }
  }
}

