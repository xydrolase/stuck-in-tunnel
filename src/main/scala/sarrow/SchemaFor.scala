package sarrow

import magnolia1.*

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import scala.jdk.CollectionConverters.*

trait SchemaFor[T] {
  def schema: Schema
  private[sarrow] def fieldTypeOf: FieldTypeOf[T]

  def buildVectorSchemaRoot(allocator: BufferAllocator = SchemaFor.rootAllocator): VectorSchemaRoot = {
    VectorSchemaRoot.create(schema, allocator)
  }
  def buildArrowWriter(allocator: BufferAllocator = SchemaFor.rootAllocator)(using indexable: Indexable[T]): ArrowWriter[T] = {
    val root = buildVectorSchemaRoot(allocator)

    val children = root.getFieldVectors().asScala.zip(fieldTypeOf.children).map { case (vector, fto) =>
      vector.allocateNew()
      fto._2.createFieldWriter(vector)
    }

    new ArrowWriter(root, children.toArray)
  }
}

object SchemaFor {
  val rootAllocator = new RootAllocator(Long.MaxValue)

  given [T](using fto: FieldTypeOf[T]): SchemaFor[T] with
    def schema: Schema = {
      require(
        fto.fieldType.getType.getTypeID() == ArrowTypeID.Struct,
        "Schema can only be derived for an Arrow Struct."
      )
      new Schema(fto.childFields)
    }

    def fieldTypeOf: FieldTypeOf[T] = fto
}
