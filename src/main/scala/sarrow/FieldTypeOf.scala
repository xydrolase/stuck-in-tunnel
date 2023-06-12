package sarrow

import magnolia1.*
import org.apache.arrow.vector.types.pojo.{FieldType, Field, ArrowType}
import org.apache.arrow.vector.types.{FloatingPointPrecision, TimeUnit => ArrowTimeUnit}

import java.{lang => jl, util => ju}
import java.time.LocalDateTime

import scala.jdk.CollectionConverters._
import scala.collection.immutable.ArraySeq
import org.apache.arrow.vector.ValueVector
import cats.Show
import org.apache.arrow.vector.VarCharVector

trait FieldTypeOf[T] {
  def fieldType: FieldType
  def isComplex: Boolean = fieldType.getType().isComplex()
  def children: Seq[(String, FieldTypeOf[_])] = Nil
  def childFields: ju.List[Field] = if (children.isEmpty) null else {
    children.map { case (name, fto) => new Field(name, fto.fieldType, fto.childFields) }.asJava
  }

  def createFieldWriter(v: ValueVector): ArrowFieldWriter = ArrowWriter.createFieldWriter(fieldType.getType(), v) 

  def toField(name: String): Field = new Field(name, fieldType, childFields)
}


object FieldTypeOf extends AutoDerivation[FieldTypeOf] { 
  def apply[T](ft: FieldType): FieldTypeOf[T] = new FieldTypeOf[T] {
    val fieldType: FieldType = ft
  }

  def apply[T](ft: FieldType, c: Seq[(String, FieldTypeOf[_])]): FieldTypeOf[T] = new FieldTypeOf[T] {
    val fieldType: FieldType = ft
    override val children: Seq[(String, FieldTypeOf[_])] = c
  }

  def withShowWriter[T: Show] = new FieldTypeOf[T] {
    val fieldType: FieldType = FieldType.nullable(new ArrowType.Utf8())
    override def createFieldWriter(v: ValueVector): ArrowFieldWriter = new ShowWriter[T](v.asInstanceOf[VarCharVector])
  }

  given FieldTypeOf[Boolean] = apply(FieldType.notNullable(new ArrowType.Bool())) 
  given FieldTypeOf[Int] = apply(FieldType.notNullable(new ArrowType.Int(32, true))) 
  given FieldTypeOf[Long] = apply(FieldType.notNullable(new ArrowType.Int(64, true))) 
  given FieldTypeOf[Float] = apply(FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))) 
  given FieldTypeOf[Double] = apply(FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))) 

  given FieldTypeOf[String] = apply(FieldType.nullable(new ArrowType.Utf8())) 

  given [T](using fto: FieldTypeOf[T]): FieldTypeOf[Option[T]] = {
    // if `fto` is already nullable, return it as is; otherwise, return a new nullable FieldType
    if (fto.fieldType.isNullable()) fto.asInstanceOf[FieldTypeOf[Option[T]]] else {
      apply(FieldType.nullable(fto.fieldType.getType()), fto.children)
    }
  }

  given [T](using fto: FieldTypeOf[T]): FieldTypeOf[Seq[T]] = {
    apply(
      FieldType.nullable(new ArrowType.List()),
      List(("element", fto))
    )
  }

  // java primitive types
  given fieldTypeOfJBool: FieldTypeOf[jl.Boolean] = apply(FieldType.nullable(new ArrowType.Bool())) 
  given fieldTypeOfJInt: FieldTypeOf[jl.Integer] = apply(FieldType.nullable(new ArrowType.Int(32, true))) 
  given fieldTypeOfJLong: FieldTypeOf[jl.Long] = apply(FieldType.nullable(new ArrowType.Int(64, true))) 
  given fieldTypeOfJFloat: FieldTypeOf[jl.Float] = apply(FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))) 
  given fieldTypeOfJDouble: FieldTypeOf[jl.Double] = apply(FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))) 

  // set timezone to null - the writer always convert time to UTC
  given fieldTypeOfLocalDateTime: FieldTypeOf[LocalDateTime] = apply(FieldType.notNullable(new ArrowType.Timestamp(ArrowTimeUnit.MILLISECOND, null))) 

  // type-class derivation
  def join[T](ctx: CaseClass[sarrow.FieldTypeOf, T]): FieldTypeOf[T] = {
    val children = new Array[(String, FieldTypeOf[_])](ctx.params.length)

    var i = 0
    ctx.params.foreach { param =>
      val ptc = param.typeclass
      val f = new Field(param.label, ptc.fieldType, ptc.childFields)
      children(i) = (param.label, ptc)
      i += 1
    }

    // TODO: revisit nullable?
    apply(FieldType.nullable(new ArrowType.Struct()), ArraySeq.unsafeWrapArray(children))
  }

  // TODO: to support coproduct
  def split[T](ctx: SealedTrait[sarrow.FieldTypeOf, T]): FieldTypeOf[T] = ???
}

