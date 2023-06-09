package stunnel.concurrent

import cats.effect.IO

import scala.concurrent.duration.Duration


trait KeyedCache[K, V] {
  def load(key: K)(fa: IO[(V, Duration)]): IO[V]
  def loadNoExpiry(key: K)(fa: IO[V]) = load(key)(fa.map { v => (v, Duration.Inf) })
  def get(key: K): IO[Option[V]]
  def put(key: K, fa: IO[(V, Duration)]): Unit
}


class ConcurrentKeyedCache[K, V] extends KeyedCache[K, V] {
  private val slots = new java.util.concurrent.ConcurrentHashMap[K, Cached[V]]()
  
  override def get(key: K): IO[Option[V]] = Option(slots.get(key)) match
    case None => IO.pure(None)
    case Some(cached) => cached.get.map(v => Option(v))

  override def load(key: K)(fa: IO[(V, Duration)]): IO[V] =
    slots.computeIfAbsent(key, _ => Cached.createUnsafe(fa)).get

  override def put(key: K, fa: IO[(V, Duration)]): Unit = slots.put(key, Cached.createUnsafe(fa))
}
