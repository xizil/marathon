package mesosphere.marathon.core.election.impl

import akka.event.EventStream
import com.codahale.metrics.MetricRegistry
import mesosphere.chaos.http.HttpConf
import mesosphere.marathon.core.base.{ CrashStrategy, ShutdownHooks }
import mesosphere.marathon.core.election.{ ElectionCandidate, ElectionService, LocalLeadershipEvent }
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.test.{ MarathonActorSupport, MarathonSpec }
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually
import org.scalatest.{ BeforeAndAfter, GivenWhenThen }

class PseudoElectionServiceTest
    extends MarathonActorSupport
    with MarathonSpec
    with GivenWhenThen
    with BeforeAndAfter
    with Eventually {

  class Fixture {
    val httpConfig: HttpConf = mock[HttpConf]
    val hostPort: String = "unresolvable:2181"
    val electionService: ElectionService = mock[ElectionService]
    val events: EventStream = new EventStream(system)
    val candidate: ElectionCandidate = mock[ElectionCandidate]
    val metrics: Metrics = new Metrics(new MetricRegistry)
    val shutdownHooks: ShutdownHooks = mock[ShutdownHooks]
    val crashStrategy: CrashStrategy = mock[CrashStrategy]
  }

  test("leader is not set initially") {
    val f = new Fixture
    val electionService = new PseudoElectionService(
      f.hostPort, system, f.events, f.metrics, f.shutdownHooks, f.crashStrategy)

    electionService.currentCandidate.get should be(None)
  }

  test("leader is eventually set after offerLeadership is called") {
    val f = new Fixture
    val electionService = new PseudoElectionService(
      f.hostPort, system, f.events, f.metrics, f.shutdownHooks, f.crashStrategy)

    Given("leadership is offered")
    electionService.offerLeadership(f.candidate)
    Then("leader is set")
    eventually { electionService.currentCandidate.get should equal(Some(f.candidate)) }

    Given("leadership is offered again")
    electionService.offerLeadership(f.candidate)

    Then("leader is set to None and Marathon stops")
    eventually { electionService.currentCandidate.get should equal(None) }
    eventually { Mockito.verify(f.crashStrategy).crash() }
  }

  test("Marathon stops after abdicateLeadership while being idle") {
    val f = new Fixture
    val electionService = new PseudoElectionService(
      f.hostPort, system, f.events, f.metrics, f.shutdownHooks, f.crashStrategy)

    Given("leadership is abdicated while not being leader")
    electionService.abdicateLeadership()

    Then("leader is None and Marathon stops")
    eventually { electionService.currentCandidate.get should be(None) }
    eventually { Mockito.verify(f.crashStrategy).crash() }
  }

  test("events are sent") {
    val f = new Fixture
    val events = mock[EventStream]

    val electionService = new PseudoElectionService(
      f.hostPort, system, events, f.metrics, f.shutdownHooks, f.crashStrategy)

    Given("this instance is becoming a leader")
    electionService.offerLeadership(f.candidate)
    eventually { electionService.currentCandidate.get should equal(Some(f.candidate)) }

    Then("the candidate is called, then an event is published")
    val order = Mockito.inOrder(events, f.candidate)
    eventually { order.verify(f.candidate).startLeadership() }
    eventually { order.verify(events).publish(LocalLeadershipEvent.ElectedAsLeader) }

    Given("this instance is abdicating")
    electionService.abdicateLeadership()

    Then("the candidate is called, then an event is published")
    eventually { order.verify(f.candidate).stopLeadership() }
    eventually { order.verify(events).publish(LocalLeadershipEvent.Standby) }

    Then("the candidate is set to None")
    eventually { electionService.currentCandidate.get should be(None) }

    Then("then Marathon stops")
    eventually { Mockito.verify(f.crashStrategy).crash() }
  }

  test("Marathon stops after leadership abdication while being a leader") {
    val f = new Fixture
    val electionService = new PseudoElectionService(
      f.hostPort, system, f.events, f.metrics, f.shutdownHooks, f.crashStrategy)

    Given("this instance becomes leader and then abdicates leadership")
    electionService.offerLeadership(f.candidate)
    eventually { electionService.currentCandidate.get should equal(Some(f.candidate)) }
    electionService.abdicateLeadership()

    Then("then state is Stopped and Marathon stops")
    eventually { electionService.currentCandidate.get should be(None) }
    eventually { Mockito.verify(f.crashStrategy).crash() }
  }

  test("Marathon stops if a candidate's startLeadership fails") {
    val f = new Fixture
    val electionService = new PseudoElectionService(
      f.hostPort, system, f.events, f.metrics, f.shutdownHooks, f.crashStrategy)

    Mockito.when(f.candidate.startLeadership()).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        throw new Exception("candidate.startLeadership exception")
      }
    })

    Given("this instance is offering leadership and candidate.startLeadership throws an exception")
    electionService.offerLeadership(f.candidate)

    Then("the instance is stopped")
    eventually { electionService.currentCandidate.get should be(None) }
    eventually { Mockito.verify(f.crashStrategy).crash() }
  }
}
