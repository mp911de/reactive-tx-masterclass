package rxtx.plain;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import rxtx.extension.Neo4jDriverExtension;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.driver.Driver;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxStatementRunner;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;

@ExtendWith(Neo4jDriverExtension.class)
@DisabledOnJre({ JRE.JAVA_8, JRE.JAVA_9, JRE.JAVA_10 }) // Neo4j 4.0 embedded is JDK 11+
public class Neo4jTransactionTests {

	@BeforeAll
	static void createConstraints(Driver driver) {

		Flux.using(driver::rxSession,
			session -> executeUpdate(session, "CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE"),
			RxSession::close
		).then().as(StepVerifier::create).verifyComplete();
	}

	@BeforeEach
	void clearDatabase(Driver driver) {

		Flux.using(driver::rxSession,
			session -> executeUpdate(session, "MATCH (n) DETACH DELETE n"),
			RxSession::close
		).then().as(StepVerifier::create).verifyComplete();
	}

	@Test
	void autoCommit(Driver driver) {

		Flux<SummaryCounters> createPersonAndMovie =
			Flux.using(driver::rxSession, session -> {
				Flux<SummaryCounters> createPerson = executeUpdate(session, "CREATE (:Person {name: 'Aaron Paul'})");
				Flux<SummaryCounters> createMovie = executeUpdate(session,
					"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})");

				return createPerson.thenMany(createMovie);
			}, RxSession::close);

		createPersonAndMovie.then().as(StepVerifier::create)
			.verifyComplete();
	}

	@Test
	void autoCommitWithFailure(Driver driver) {

		Flux<SummaryCounters> createPersonAndMovie =
			Flux.using(driver::rxSession, session -> {
				Flux<SummaryCounters> createPerson = executeUpdate(session, "CREATE (:Person {name: 'Aaron Paul'})");
				Flux<SummaryCounters> createMovie = executeUpdate(session,
					"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})");

				return createPerson.thenMany(createMovie);
			}, RxSession::close);

		createPersonAndMovie.then().as(StepVerifier::create)
			.verifyComplete();

		Flux.using(driver::rxSession, session -> executeUpdate(session, "CREATE (:Person {name: 'Aaron Paul'})"),
			RxSession::close)
			.as(StepVerifier::create)
			.verifyErrorMatches(t -> t instanceof ClientException && t.getMessage()
				.endsWith("already exists with label `Person` and property `name` = 'Aaron Paul'"));
	}

	@Test
	void transactionalWithRollback(Driver driver) {

		RxSession session = driver.rxSession();
		Flux.from(session.beginTransaction())
			.flatMap(tx -> {
				Flux<SummaryCounters> createPerson = executeUpdate(tx, "CREATE (:Person {name: 'Aaron Paul'})");
				Flux<SummaryCounters> createMovie = executeUpdate(tx,
					"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})");

				createPerson.thenMany(createMovie).then().as(StepVerifier::create).verifyComplete();
				return tx.rollback();
			}).as(StepVerifier::create).verifyComplete();

		Flux.from(session.run("MATCH (p:Person) RETURN p.name AS name").records())
			.map(r -> r.get("name").asString())
			.as(StepVerifier::create)
			.verifyComplete();

		StepVerifier.create(session.close()).verifyComplete();
	}

	@Test
	void transactionalWithRollbackBetterVariant(Driver driver) {

		RxSession session = driver.rxSession();

		Flux.usingWhen(session.beginTransaction(),
			tx -> {
				Flux<SummaryCounters> createPerson = executeUpdate(tx, "CREATE (:Person {name: 'Aaron Paul'})");
				Flux<SummaryCounters> createMovie = executeUpdate(tx,
					"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})");

				return createPerson.concatWith(createMovie)
					.collect(AtomicInteger::new,
						(hlp, counters) -> hlp.accumulateAndGet(counters.nodesCreated(), (a, b) -> a + b))
					.map(AtomicInteger::get)
					.doOnNext(totalNumberOfNodesCreated -> {
						if (totalNumberOfNodesCreated == 2) {
							throw new RuntimeException("Throwing a business error");
						}
					});
			},
			RxTransaction::commit,
			(tx, exception) -> tx.rollback(),
			RxTransaction::commit
		).as(StepVerifier::create).verifyErrorMessage("Throwing a business error");

		Flux.from(session.run("MATCH (p:Person) RETURN p.name AS name").records())
			.map(r -> r.get("name").asString())
			.as(StepVerifier::create)
			.verifyComplete();

		StepVerifier.create(session.close()).verifyComplete();
	}

	@Test
	void cancellationEffectsDangerousWithAutoCommit(Driver driver) throws InterruptedException {

		Flux<Integer> createNodes = Flux.using(
			driver::rxSession,
			session -> session.run("UNWIND range (1,1000) AS i CREATE (s:SomeNode {position: i}) return s").records(),
			RxSession::close
		).map(r -> r.get("s").get("position").asInt());

		createNodes
			.as(StepVerifier::create)
			.expectNext(1, 2, 3, 4, 5)
			.thenCancel()
			.verify();

		// Give the completation signal some time.
		Thread.sleep(2_000L);

		Flux.using(
			driver::rxSession,
			session -> session.run("MATCH (s:SomeNode) RETURN count(s) as cnt").records(),
			RxSession::close
		)
			.map(r -> r.get("cnt").asLong())
			.single().as(StepVerifier::create).expectNext(1000L).verifyComplete();
	}

	private static Flux<SummaryCounters> executeUpdate(RxStatementRunner runner, String cypher) {

		return Flux.defer(() -> runner.run(cypher).summary()).map(ResultSummary::counters);
	}
}
