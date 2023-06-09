package stunnel.concurrent

import scala.concurrent.duration.{Duration, FiniteDuration}
import cats.effect.IO
import cats.effect.kernel.{Clock, Deferred, Ref}
import cats.implicits.*
import java.util.concurrent.TimeUnit

trait Cached[A] {
  def get: IO[A]
  def expire: IO[Unit]
}

object Cached {
  enum State[A]:
    case NoValue extends State[Nothing]
    case Value(v: A, expiresAfter: Option[FiniteDuration]) extends State[A]
    case Updating(deferred: Deferred[IO, Either[Throwable, A]]) extends State[A]

  object State:
    def empty[A]: State[A] = State.NoValue.asInstanceOf[State[A]]

  private def createFromState[A](fa: IO[(A, Duration)], state: Ref[IO, State[A]]): Cached[A] = {
    new Cached[A] {
      override def get: IO[A] = Clock[IO].monotonic.flatMap { timestamp =>
        state.modify { 
          // if the state is empty, compute a new one
          case State.NoValue => updating()
          // if the state has expired already, evict the state and compute a new one
          case _ @ State.Value(_, expiry) if expiry.exists(_ <= timestamp) => updating()
          case st @ State.Value(v, _) => st-> IO.pure(v)
          case st @ State.Updating(await) => st -> await.get.rethrow
        }.flatten
      }

      override def expire: IO[Unit] = state.update {
        case st @ State.NoValue => st
        case State.Value(_, _) => State.empty[A]
        // in-flight state should not be expired
        case st @ State.Updating(_) => st
      }

      def updating(): (State[A], IO[A]) = {
        val await = Deferred.unsafe[IO, Either[Throwable, A]]
        State.Updating(await) -> compute(await).rethrow
      }

      /**
       * Compute a new value to be cached using the provided effect `fa`.
       */
      def compute(deferred: Deferred[IO, Either[Throwable, A]]): IO[Either[Throwable, A]] = {
        val action = fa.attempt.onCancel {
          state.set(State.empty[A]) >> deferred.complete(Left(new InterruptedException())).void
        }

        IO.uncancelable[Either[Throwable, A]] { poll =>
          for {
            // the action is the only cancelable region
            result: Either[Throwable, (A, Duration)] <- poll(action)
            // get the value from the computed result
            value = result.map(_._1)
            clock <- Clock[IO].monotonic
            _ <- state.set {
              result match {
                case Left(t) => 
                  State.empty[A]
                case Right((v, expiry)) if !expiry.isFinite =>
                  State.Value(v, None)
                case Right((v, expiry)) =>
                  State.Value(v, Some(clock + FiniteDuration(expiry.toNanos, TimeUnit.NANOSECONDS)))
              }
            }
            _ <- deferred.complete(value)
          } yield value
        }
      }
    }
  }

  def create[A](fa: IO[(A, Duration)]): IO[Cached[A]] = Ref.of[IO, State[A]](State.empty[A]).map { state =>
    createFromState(fa, state)
  }

  def createUnsafe[A](fa: IO[(A, Duration)]) = createFromState(fa, Ref.unsafe(State.empty[A]))
}
