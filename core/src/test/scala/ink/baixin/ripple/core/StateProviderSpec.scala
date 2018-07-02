package ink.baixin.ripple.core

import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec

class StateProviderSpec extends FlatSpec {
  private val testConfig = ConfigFactory.load.getConfig("ripple-test")
  val provider = new StateProvider("test-project", testConfig)
  val alterProvider = new StateProvider("test-project", testConfig)

  it should "initialize and ensure state successfully" in {
    assert(provider.ensureState)
  }

  it should "listen state change successfully" in {
    val listener = provider.listener
    val prev = listener.getState

    assert(prev.isDefined)

    // perform some mutation
    val task = listener.start
    Thread.sleep(5000) // wait for new state to be fetched
    task.cancel
    assert(listener.getState.get.segments.size > prev.get.segments.size)
  }

  it should "mutate state as expected" in {
    val mutator = provider.mutator
    val prev = provider.listener.syncAndGetState.get
    var reservedId = prev.reservedId

    var ns = mutator.addSegments(4).get
    assert(ns.reservedId >= reservedId + 4)
    assert(ns.segments.length - prev.segments.size == 4)

    val idSets = ns.segments.map(_.id).toSet
    ns = mutator.provisionSegments(idSets, true).get
    assert(idSets.forall(id => ns.segments.exists(s => s.id == id && s.provisioned)))

    ns = mutator.provisionSegments(idSets, false).get
    assert(idSets.forall(id => ns.segments.exists(s => s.id == id && !s.provisioned)))

    ns = mutator.aggregateSegments(idSets, true).get
    assert(idSets.forall(id => ns.segments.exists(s => s.id == id && s.aggregated)))

    ns = mutator.aggregateSegments(idSets, false).get
    assert(idSets.forall(id => ns.segments.exists(s => s.id == id && !s.aggregated)))

    reservedId = ns.reservedId

    val ids = mutator.reserveIds(50)
    assert(ids.length == 50)

    ns = provider.listener.syncAndGetState.get
    assert(ns.reservedId >= reservedId + 50)
    assert(ns.segments.size == ns.segments.map(_.id).toSet.size)

    val (reserve, deletes) = ns.segments.splitAt(ns.segments.length / 2)
    ns = mutator.removeSegments(deletes.map(_.id).toSet).get
    assert(reserve.forall(s => ns.segments.exists(_.id == s.id)))
    assert(deletes.forall(s => !ns.segments.exists(_.id == s.id)))

    ns = mutator.cleanUpSegments.get
    assert(ns.segments.isEmpty)

    ns = mutator.ensureSegments.get
    assert(!ns.segments.isEmpty)

    // it should not add new segments immediately after one ensure
    assert(mutator.ensureSegments.get == ns)
  }

  it should "resolve resource correctly" in {
    val listener = provider.listener
    val resolver = provider.resolver
    val mutator = provider.mutator

    val state = listener.syncAndGetState.get
    assert(resolver.getUserTableName(state) == "test-project-sessions-users")

    assert(state.segments.length > 0) // make sure have segments
    for (seg <- state.segments) {
      assert(resolver.getFactTableName(state, seg) == s"${state.project}-${state.factTable}-${seg.id}")
    }

    assert(resolver.getUserTable.isDefined) // should be able to resolve user table

    mutator.addSegments(4) // add some segments
    val (notp, provisioned) = state.segments.splitAt(state.segments.length / 2)
    mutator.provisionSegments(provisioned.map(_.id).toSet, true)

    // before marked as provisioned, it should not resolve fact table
    for (seg <- notp) {
      val table = resolver.getFactTable(seg.startTime) // start time is exclusively
      assert(table.isEmpty)
    }

    for (seg <- provisioned) {
      val table = resolver.getFactTable(seg.startTime)
      assert(table.isDefined)
    }
  }
}
