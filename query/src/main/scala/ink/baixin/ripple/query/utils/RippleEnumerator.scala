package ink.baixin.ripple.query.utils

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.calcite.linq4j.Enumerator

class RippleEnumerator[T](
                         flag: AtomicBoolean, getIterator: => Iterator[T]
                       )(transform: (T) => Array[AnyRef]) extends Enumerator[AnyRef] {

  private var curIter = getIterator
  private var curVal: Option[T] = None

  override def current() = {
    if (curVal.isEmpty) {
      throw new java.util.NoSuchElementException()
    }
    val res = transform(curVal.get)
    if (res.size == 1) res(0) else res
  }

  override def moveNext() = {
    if (flag.get == true) {
      false
    } else if (curIter == null) {
      false
    } else if (!curIter.hasNext) {
      false
    } else {
      curVal = Some(curIter.next)
      true
    }
  }

  override def reset() {
    curIter = getIterator
  }

  override def close() {
    curIter = null
    curVal = None
  }
}

object RippleEnumerator {
  def apply[T](flag: AtomicBoolean, getIterator: => Iterator[T])(transform: (T) => Array[AnyRef]) = {
    new RippleEnumerator[T](flag, getIterator)(transform)
  }
}