package ink.baixin.ripple.scheduler

package object executors {
  val map = Seq(
    Print,
    // ripple jobs
    RippleDataRefresh
  ).map(_.name -> e).toMap

  def get(name: String): Option[TaskExecutor] = {
    if (map.contains(name)) Some(map(name))
    else None
  }
}