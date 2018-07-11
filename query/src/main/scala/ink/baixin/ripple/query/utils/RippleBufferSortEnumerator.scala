package ink.baixin.ripple.query.utils

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.calcite.linq4j.Enumerator

abstract class RippleBufferSortEnumerator[T](
                                            flag: AtomicBoolean, bufferSize: Int, getIterator: => Iterator[T]
                                          ) extends Enumerator[AnyRef] with java.util.Comparator[T] {

  private var curIter = getIterator
  private var bufferQueue = new java.util.PriorityQueue[T](bufferSize, this)

  override def current() = {
    if (bufferQueue.isEmpty) {
      throw new java.util.NoSuchElementException()
    }
    val res = transform(bufferQueue.peek)
    if (res.size == 1) res(0) else res
  }

  override def moveNext() = {
    if (flag.get == true) {
      false
    } else if (curIter == null) {
      false
    } else {
      if (!bufferQueue.isEmpty) bufferQueue.poll
      fillBuffer

      !bufferQueue.isEmpty
    }
  }

  private def fillBuffer {
    while (curIter.hasNext && bufferQueue.size < bufferSize) {
      bufferQueue.offer(curIter.next)
    }
  }

  override def reset() {
    curIter = getIterator
  }

  override def close() {
    curIter = null
    bufferQueue.clear
    bufferQueue = null
  }

  protected def transform(item: T): Array[AnyRef]
}