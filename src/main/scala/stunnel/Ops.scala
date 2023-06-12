package stunnel

import fs2.{Stream, Pipe, Pull}
import fs2.io.file.Path
import cats.effect.{IO, Resource}
import cats.effect.kernel.Ref
import cats.effect.std.Queue
import org.locationtech.jts.geom.LineSegment

import java.io.FileOutputStream
import java.nio.channels.Channels

import stunnel.geometry.{given, *}
import stunnel.njtransit.VehicleLocation
import sarrow.{SchemaFor, ArrowWriter, Indexable}
import org.apache.arrow.vector.ipc.ArrowFileWriter
import cats.effect.std.Hotswap

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
  }

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

    // `in` is Stream[IO, VehicleLocation]
    in => go(in, new LRUCache(maxCacheSize)).stream
  }

  def publishVehicleLocation(queue: Queue[IO, VehicleLocation]): Pipe[IO, VehicleLocation, VehicleLocation] = {
    in => in.evalTap { vecLoc =>
      queue.offer(vecLoc)
    }
  }

  def writeArrowRotate[T: SchemaFor: Indexable](computePath: IO[Path], limit: Int, batchSize: Int): Pipe[IO, T, Path] = {
    val schemaFor = summon[SchemaFor[T]]

    def newFileWriter(writer: ArrowWriter[T]): Resource[IO, (Path, ArrowFileWriter)] =
      Resource
        .eval(computePath)
        .flatMap { p => 
          Resource.make(IO.blocking {
            val fos = new FileOutputStream(p.toString)
            val fileWriter = new ArrowFileWriter(writer.root, null, Channels.newChannel(fos))
            fileWriter.start()
            (p, fileWriter)
          })(pathAndWriter => IO.blocking(pathAndWriter._2.close()))
        }

    def go(writerHotswap: Hotswap[IO, (Path, ArrowFileWriter)], delegate: WriterDelegate[T],
           batchAcc: Int, fileAcc: Int, s: Stream[IO, T]): Pull[IO, Path, Unit] = {

      val toWrite = (limit - fileAcc).min(batchSize - batchAcc).min(Int.MaxValue)
      s.pull.unconsLimit(toWrite).flatMap {
        case None => Pull.done
        case Some((hd, tl)) =>
          val newBatchAcc = batchAcc + hd.size
          val newFileAcc = fileAcc + hd.size

          val append = Pull.eval {
            IO.println(s"Writing a batch of ${hd.size} inputs to writer") *> IO(hd.foreach(row => delegate.write(row)))
          }

          // check if we exceed the batch size, or the file limit
          if (newFileAcc >= limit) {
            append >> 
              // must flush the current batch before closing the file
              Pull.eval(delegate.writeBatch()) >> 
              // output the current file to be closed
              Pull.output1(delegate.path) >> 
              Pull.eval {
                writerHotswap
                  .swap(newFileWriter(delegate.writer))
                  .map { case (path, fileWriter) => WriterDelegate(delegate.writer, fileWriter, path) }
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
        .map { case (hotswap, (path, fw)) => (hotswap, WriterDelegate(writer, fw, path)) }
      stream <- go(hotswap, delegate, 0, 0, in).stream
    } yield stream
  }
  
}
