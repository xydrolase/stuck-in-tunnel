package sarrow

/**
 * Adapted and modified from ArrowWriter.scala from Apache Spark,
 * see: https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowWriter.scala
 */

import cats.Show

import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.FloatingPointPrecision

import org.apache.arrow.dataset.file.JniWrapper
import org.apache.arrow.c.{Data, ArrowArray, ArrowArrayStream}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.dataset.file.{FileFormat, DatasetFileWriter}

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.time.LocalDateTime
import java.time.ZoneId

import scala.jdk.CollectionConverters.*

object ArrowWriter {
  def create[T: Indexable](root: VectorSchemaRoot): ArrowWriter[T] = {
    val children = root.getFieldVectors().asScala.map { vector =>
      vector.allocateNew()
      createFieldWriter(vector)
    }

    new ArrowWriter(root, children.toArray)
  }

  private[sarrow] def createFieldWriter(tpe: ArrowType, vector: ValueVector): ArrowFieldWriter = (tpe, vector) match {
      case (_: ArrowType.Bool, v: BitVector) => new BooleanWriter(v)
      case (tpe: ArrowType.Int, v: TinyIntVector) if tpe.getBitWidth() == 8 => new ByteWriter(v)
      case (tpe: ArrowType.Int, v: SmallIntVector) if tpe.getBitWidth() == 16 => new ShortWriter(v)
      case (tpe: ArrowType.Int, v: IntVector) if tpe.getBitWidth() == 32 => new IntegerWriter(v)
      case (tpe: ArrowType.Int, v: BigIntVector) if tpe.getBitWidth() == 64 => new LongWriter(v)
      case (tpe: ArrowType.FloatingPoint, v: Float4Vector) if tpe.getPrecision() == FloatingPointPrecision.SINGLE => new FloatWriter(v)
      case (tpe: ArrowType.FloatingPoint, v: Float8Vector) if tpe.getPrecision() == FloatingPointPrecision.DOUBLE => new DoubleWriter(v)
      case (_: ArrowType.Utf8, v: VarCharVector) => new StringWriter(v)
      case (_: ArrowType.Binary, v: VarBinaryVector) => new BinaryWriter(v)
      case (_: ArrowType.Null, v: NullVector) => new NullWriter(v)
      case (_: ArrowType.Duration, v: DurationVector) => new DurationWriter(v)
      case (_: ArrowType.Timestamp, v: TimeStampMilliVector) => new TimestampNTZWriter(v)
      case (at, _) =>
        throw new IllegalArgumentException(s"Unsupported ArrowType $at")
  }

  private[sarrow] def createFieldWriter(vector: ValueVector): ArrowFieldWriter = {
    val field = vector.getField()
    createFieldWriter(field.getType(), vector)
  }

}


/**
 * A writer that writes input row-wise data of type [[T]] into the columnar table `root` (a [[VectorSchemaRoot]]).
 * 
 */
class ArrowWriter[T: Indexable](val root: VectorSchemaRoot, fields: Array[ArrowFieldWriter]) {
  private var count: Int = 0
  private val indexable = summon[Indexable[T]]

  def isDirty: Boolean = count > 0

  def write(input: T): Unit = {
    var i = 0
    while (i < fields.size) {
      fields(i).write(indexable.toAccessor(input), i)
      i += 1
    }
    count += 1
  }

  def writeChunk(chunk: Iterable[T]): Unit = {
    chunk.foreach(write)
  }

  def finish(): Unit = {
    root.setRowCount(count)
    fields.foreach(_.finish())
  }

  def reset(): Unit = {
    root.setRowCount(0)
    count = 0
    fields.foreach(_.reset())
  }

  def writeToParquet(uri: String,
      partitionColumns: Array[String] = new Array[String](0),
      maxPartitions: Int = 1024,
      baseNameTemplate: String = "data_{i}",
      allocator: BufferAllocator = SchemaFor.rootAllocator): Unit = {
    
    DatasetFileWriter.write(allocator, new ArrowVSRReader(root, allocator),   
      FileFormat.PARQUET, uri, partitionColumns, maxPartitions, baseNameTemplate)
  }
}

private[sarrow] abstract class ArrowFieldWriter {
  def valueVector: ValueVector

  def name: String = valueVector.getField().getName()
  def nullable: Boolean = valueVector.getField().isNullable()

  def setNull(): Unit
  def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit

  private[sarrow] var count: Int = 0

  def write(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    if (accessor.isNullAt(ordinal)) {
      setNull()
    } else {
      setValue(accessor, ordinal)
    }
    count += 1
  }

  def finish(): Unit = {
    valueVector.setValueCount(count)
  }

  def reset(): Unit = {
    valueVector.reset()
    count = 0
  }

  // TODO: close the underlying ValueVector?
}

private[sarrow] class BooleanWriter(val valueVector: BitVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    valueVector.setSafe(count, if (accessor.getAs[Boolean](ordinal)) 1 else 0)
  }
}

private[sarrow] class ByteWriter(val valueVector: TinyIntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    valueVector.setSafe(count, accessor.getAs[Byte](ordinal))
  }
}

private[sarrow] class ShortWriter(val valueVector: SmallIntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    valueVector.setSafe(count, accessor.getAs[Short](ordinal))
  }
}

private[sarrow] class IntegerWriter(val valueVector: IntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    valueVector.setSafe(count, accessor.getAs[Int](ordinal))
  }
}

private[sarrow] class LongWriter(val valueVector: BigIntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    valueVector.setSafe(count, accessor.getAs[Long](ordinal))
  }
}

private[sarrow] class FloatWriter(val valueVector: Float4Vector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    valueVector.setSafe(count, accessor.getAs[Float](ordinal))
  }
}

private[sarrow] class DoubleWriter(val valueVector: Float8Vector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    valueVector.setSafe(count, accessor.getAs[Double](ordinal))
  }
}

private[sarrow] class StringWriter(val valueVector: VarCharVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    val bytes = accessor.getAs[String](ordinal).getBytes()
    val buffer = ByteBuffer.wrap(bytes)
    valueVector.setSafe(count, buffer, buffer.position(), bytes.length)
  }
}

private [sarrow] class ShowWriter[T](val valueVector: VarCharVector)(using show: Show[T]) extends ArrowFieldWriter {
  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    val bytes = show.show(accessor.getAs[T](ordinal)).getBytes()
    val buffer = ByteBuffer.wrap(bytes)
    valueVector.setSafe(count, buffer, buffer.position(), bytes.length)
  }
}

/*
private[sarrow] class LargeStringWriter(
    val valueVector: LargeVarCharVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    val utf8ByteBuffer = utf8.getByteBuffer
    // todo: for off-heap UTF8String, how to pass in to arrow without copy?
    valueVector.setSafe(count, utf8ByteBuffer, utf8ByteBuffer.position(), utf8.numBytes())
  }
}

private[arrow] class LargeBinaryWriter(
    val valueVector: LargeVarBinaryVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueVector.setSafe(count, bytes, 0, bytes.length)
  }
}
*/

private[sarrow] class BinaryWriter(
    val valueVector: VarBinaryVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    val bytes = accessor.getAs[Array[Byte]](ordinal)
    valueVector.setSafe(count, bytes, 0, bytes.length)
  }
}

private[sarrow] class TimestampNTZWriter(
    val valueVector: TimeStampMilliVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    val ts = accessor.get(ordinal) match {
      case ldt: LocalDateTime => 
        ldt.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()
      case invalid => throw new IllegalArgumentException(s"Cannot handle data type ${invalid.getClass.getName} for TimeStampVector")
    }
    valueVector.setSafe(count, ts)
  }
}

/*
private[arrow] class DateWriter(val valueVector: DateDayVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[arrow] class TimestampWriter(
    val valueVector: TimeStampMicroTZVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

*/

/*
private[sarrow] class ArrayWriter(
    val valueVector: ListVector,
    val elementWriter: ArrowFieldWriter) extends ArrowFieldWriter {

  override def setNull(): Unit = {}

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    val array = accessor.get(ordinal)
    var i = 0
    valueVector.startNewValue(count)
    while (i < array.numElements()) {
      elementWriter.write(array, i)
      i += 1
    }
    valueVector.endValue(count, array.numElements())
  }

  override def finish(): Unit = {
    super.finish()
    elementWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    elementWriter.reset()
  }
}

private[sarrow] class StructWriter(
    val valueVector: StructVector,
    children: Array[ArrowFieldWriter]) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).setNull()
      children(i).count += 1
      i += 1
    }
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val struct = input.getStruct(ordinal, children.length)
    var i = 0
    valueVector.setIndexDefined(count)
    while (i < struct.numFields) {
      children(i).write(struct, i)
      i += 1
    }
  }

  override def finish(): Unit = {
    super.finish()
    children.foreach(_.finish())
  }

  override def reset(): Unit = {
    super.reset()
    children.foreach(_.reset())
  }
}

private[sarrow] class MapWriter(
    val valueVector: MapVector,
    val structVector: StructVector,
    val keyWriter: ArrowFieldWriter,
    val valueWriter: ArrowFieldWriter) extends ArrowFieldWriter {

  override def setNull(): Unit = {}

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val map = input.getMap(ordinal)
    valueVector.startNewValue(count)
    val keys = map.keyArray()
    val values = map.valueArray()
    var i = 0
    while (i <  map.numElements()) {
      structVector.setIndexDefined(keyWriter.count)
      keyWriter.write(keys, i)
      valueWriter.write(values, i)
      i += 1
    }

    valueVector.endValue(count, map.numElements())
  }

  override def finish(): Unit = {
    super.finish()
    keyWriter.finish()
    valueWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    keyWriter.reset()
    valueWriter.reset()
  }
}
*/

private[sarrow] class NullWriter(val valueVector: NullVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {}

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {}
}


private[sarrow] class DurationWriter(val valueVector: DurationVector) extends ArrowFieldWriter {
  private val timeUnit = valueVector.getUnit()

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(accessor: Indexable.Accessor[_], ordinal: Int): Unit = {
    accessor.get(ordinal) match {
      case d: java.time.Duration =>
        val value = timeUnit match
          case org.apache.arrow.vector.types.TimeUnit.SECOND => d.toSeconds()
          case org.apache.arrow.vector.types.TimeUnit.MILLISECOND => d.toMillis()
          case org.apache.arrow.vector.types.TimeUnit.MICROSECOND => d.toMillis() * 1000L
          case org.apache.arrow.vector.types.TimeUnit.NANOSECOND => d.toNanos()

        valueVector.setSafe(count, value)
        
      case d: scala.concurrent.duration.Duration =>
        val value = timeUnit match
          case org.apache.arrow.vector.types.TimeUnit.SECOND => d.toSeconds
          case org.apache.arrow.vector.types.TimeUnit.MILLISECOND => d.toMillis
          case org.apache.arrow.vector.types.TimeUnit.MICROSECOND => d.toMicros
          case org.apache.arrow.vector.types.TimeUnit.NANOSECOND => d.toNanos

        valueVector.setSafe(count, value)
      case invalid =>
        throw new IllegalArgumentException(s"Unsupported data type: ${invalid.getClass.getName}")
    }
  }
}
