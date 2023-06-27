package stunnel

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import fs2.io.file.{Files, Path}
import java.nio.file.{Files => NioFiles}
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import stunnel.njtransit.*

import java.time.LocalDateTime
import java.nio.channels.Channels
import java.io.FileInputStream
import scala.concurrent.duration._
import scala.util.Try
import sarrow.SchemaFor

class OpsSpec extends AnyWordSpec with Matchers {
  def tallyRecords(path: Path): Int = {
    var reader: ArrowFileReader = null
    try {
      reader = new ArrowFileReader(NioFiles.newByteChannel(path.toNioPath), SchemaFor.rootAllocator)
      var n = 0
      while (reader.loadNextBatch()) {
        n += reader.getVectorSchemaRoot().getRowCount()
      }
      
      n
    } finally {
      if (reader ne null) reader.close()
    }
  }

  "an Ops" when {
    "tracking vehicle movements" should {
      "return a stream of vehicle movements" in {
        val vehicles = Stream(
          VehicleLocation(1440, LocalDateTime.now(), 34f, -73f, 123, "126", 217, 8,
            "NEW YORK", false, "light", 133579, LocalDateTime.now()
            ),
          VehicleLocation(1440, LocalDateTime.now(), 34.51f, -72.8f, 123, "126", 217, 8,
            "NEW YORK", false, "light", 133579, LocalDateTime.now()
            ),
          VehicleLocation(1733, LocalDateTime.now(), 34.11f, -73.8f, 159, "159", 217, 8,
            "NEW YORK", false, "light", 672579, LocalDateTime.now()
            )
          ).covary[IO]

        val movements = vehicles.through(Ops.trackMovement(5)).compile.toList.unsafeRunSync() 
        movements.length shouldBe 1
        movements.head.currentLocation.tripId shouldBe 133579
      }
    }

    "writing records to Arrow IPC file" should {
      "ensure all records are flushed to file when stream ends" in {
        val N = 10
        val vehicles = Stream.eval(
          IO.sleep(100.millis) *> IO(VehicleLocation(1440, LocalDateTime.now(), 34f, -73f, 123, "126", 217, 8,
            "NEW YORK", false, "light", 133579, LocalDateTime.now()
            ))
        ).repeat.take(N)

        val path = Files[IO].tempFile(None, "vecloc_", ".arrow", permissions = None).use(p => IO.pure(p))
        val output = vehicles.through(Ops.writeArrowRotate(path, limit = 100, batchSize = 50)).compile.toList.unsafeRunSync()
        output.length shouldBe 1

        tallyRecords(output.head) shouldBe N
      }

      "ensure safe resource release by flush and close file" in {
        val N = 5
        val vehicles = Stream.eval(
          IO.sleep(100.millis) *> IO(VehicleLocation(1440, LocalDateTime.now(), 34f, -73f, 123, "126", 217, 8,
            "NEW YORK", false, "light", 133579, LocalDateTime.now()
            ))
        ).repeat.zipWithIndex.flatMap { case (vecLoc, i) =>
          if (i < N) Stream.emit(vecLoc)
          else Stream.raiseError[IO](new RuntimeException("Unable to retrieve vehicle location"))
        }

        val path = IO.pure(Path.fromNioPath(NioFiles.createTempFile("test", ".arrow")))
        val output = Try(vehicles.through(Ops.writeArrowRotate(path, limit = 100, batchSize = 50)).compile.toList.unsafeRunSync())
        output.isFailure shouldBe true

        path.map(p => tallyRecords(p)).unsafeRunSync() shouldBe N
      }
    }
  }
}
