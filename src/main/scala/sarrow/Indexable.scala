package sarrow

import sarrow.Indexable.Accessor

trait Indexable[T] {
  def get(input: T, ordinal: Int): Any
  def length(input: T): Int
  def isNullAt(input: T, ordinal: Int): Boolean = {
    val v = get(input, ordinal)
    // handle Option[_]
    v == null || v == None
  }
  def getAs[A](input: T, ordinal: Int): A = get(input, ordinal).asInstanceOf[A]

  def toAccessor(input: T): Accessor[T] = Accessor(this, input)
}

object Indexable {
  def apply[T: Indexable]: Indexable[T] = summon[Indexable[T]]

  inline def unwrap(v: Any): Any = v match
    // unwrap Option[_]
    case Some(v) => v
    case None => null
    case v => v

  given productIndexable[T <: Product]: Indexable[T] with
    def get(input: T, ordinal: Int): Any = unwrap(input.productElement(ordinal))
    def length(input: T): Int = input.productArity

  given tupleIndexable[L, R](using idxL: Indexable[L], idxR: Indexable[R]): Indexable[(L, R)] = new:
    def get(input: (L, R), ordinal: Int): Any = {
      val ll = idxL.length(input._1)
      if (ordinal < ll) idxL.get(input._1, ordinal)
      else idxR.get(input._2, ordinal - ll)
    }

    def length(input: (L, R)): Int = idxL.length(input._1) + idxR.length(input._2)


  case class Accessor[T](indexable: Indexable[T], input: T) {
    def get(ordinal: Int): Any = indexable.get(input, ordinal)
    def isNullAt(ordinal: Int): Boolean = indexable.isNullAt(input, ordinal)
    def getAs[A](ordinal: Int): A = indexable.getAs[A](input, ordinal) 
  }
}
