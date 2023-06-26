package sarrow

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.apache.arrow.dataset.file.DatasetFileWriter
import org.apache.arrow.vector.ipc.ArrowFileWriter
import java.io.FileOutputStream
import java.nio.channels.Channels

import stunnel.njtransit.{Point, Pattern, Stop}
import stunnel.geometry.GeoUtilsSpec

import scala.jdk.CollectionConverters.*

class SchemaForSpec extends AnyWordSpec with Matchers {
  "a SchemaFor" should {
    "be auto-derivable from case classes" in {
      val p = Point(0, 1f, 2f, Stop, None, None, 10f)
      summon[SchemaFor[Point]].schema.getFields().asScala
       .map(_.getName) should contain theSameElementsInOrderAs p.productElementNames.toSeq
    }

    "support building an ArrowWriter" in {
      val pattern = GeoUtilsSpec.loadResource[Seq[Pattern]]("/patterns.json").head

      val writer: ArrowWriter[Point] = summon[SchemaFor[Point]].buildArrowWriter()

      val out = new FileOutputStream("/tmp/test.arrow")
      val dsWriter = new ArrowFileWriter(writer.root, null, Channels.newChannel(out))

      dsWriter.start()

      writer.writeChunk(pattern.points)
      writer.finish()

      dsWriter.writeBatch()
      dsWriter.end()
    }

    /*
    "support building an ArrowWriter 2" in {
      val writer: ArrowWriter[Athlete] = summon[SchemaFor[Athlete]].buildArrowWriter()

      writer.write(Athlete("alex", 27, 275.3))
      writer.write(Athlete("christine", 24, 314.3))
      writer.write(Athlete("henry", 22, 337.9))
      writer.finish()

      writer.writeToParquet("file:///tmp/test_direct.parquet")
    }
    */
  }
}
