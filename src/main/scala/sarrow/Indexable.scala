package sarrow

import sarrow.Indexable.Accessor

trait Indexable[T] {
  def get(input: T, ordinal: Int): Any
  def isNullAt(input: T, ordinal: Int): Boolean = {
    val v = get(input, ordinal)
    // handle Option[_]
    v == null || v == None
  }
  def getAs[A](input: T, ordinal: Int): A = get(input, ordinal).asInstanceOf[A]

  def toAccessor(input: T): Accessor[T] = Accessor(this, input)
}

object Indexable {
  inline def unwrap(v: Any): Any = v match
    // unwrap Option[_]
    case Some(v) => v
    case None => null
    case v => v

  given productIndexable[T <: Product]: Indexable[T] with
    def get(input: T, ordinal: Int): Any = unwrap(input.productElement(ordinal))

  // given iterableIndexable[F[_] <: Iterable[_]]: Indexable[F[_]]

  case class Accessor[T](indexable: Indexable[T], input: T) {
    def get(ordinal: Int): Any = indexable.get(input, ordinal)
    def isNullAt(ordinal: Int): Boolean = indexable.isNullAt(input, ordinal)
    def getAs[A](ordinal: Int): A = indexable.getAs[A](input, ordinal) 
  }
}
