package ink.baixin.ripple.scheduler

package object executors {
  val map = Seq(
    Print,
    // ripple jobs
    RippleDataRefresh,

    // planning tasks
    PlanDataRefresh,

    // hive related tasks
    HiveTableRefresh,

    // kylin related tasks
    KylinCubeBuild,
    KylinCubeRefresh,
    KylinCubeCacheUpdate
  ).map(e => e.name -> e).toMap

  def get(name: String): Option[TaskExecutor] = {
    if (map.contains(name)) Some(map(name))
    else None
  }
}