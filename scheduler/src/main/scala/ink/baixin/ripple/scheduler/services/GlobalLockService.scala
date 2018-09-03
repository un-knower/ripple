package ink.baixin.ripple.scheduler.services

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

object GlobalLockService {
  val lock = new ReentrantReadWriteLock()

  def acquire(blocking: Boolean) =
    if (blocking) lock.writeLock().tryLock(60, TimeUnit.MINUTES)
    else lock.readLock().tryLock(90, TimeUnit.MINUTES)

  def release(blocking: Boolean) =
    if (blocking) lock.writeLock().unlock()
    else lock.readLock().unlock()
}